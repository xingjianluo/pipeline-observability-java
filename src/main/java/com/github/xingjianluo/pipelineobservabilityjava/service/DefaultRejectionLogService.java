package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.xingjianluo.pipelineobservabilityjava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectionRecord;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/**
 * 进程内留痕实现：明细有界、计数精确。
 * - 明细保存在容量上限为 rejectionLogCapacity 的有界队列中，超出后按到达先后
 *   淘汰最旧明细（FIFO），内存不随接入方与拒绝量无限增长；
 * - 按管道、按原因的累计被拒条数用 AtomicLong 独立累加，只增不减、绝不随明细
 *   淘汰而减少，并发下不丢不重、始终精确；
 * - 明细被淘汰后，按管道/原因的明细查询照常可用（返回仍保留的明细），
 *   汇总查询（countByCode/totalCount）始终反映全量累计。
 */
@Service
public class DefaultRejectionLogService implements RejectionLogService {

    private static final Logger log = LoggerFactory.getLogger(DefaultRejectionLogService.class);
    private static final int MAX_SNIPPET = 500;
    /** 无法解析出管道名时的归组键（正常管道名不会与之冲突）。 */
    public static final String UNKNOWN_PIPELINE = "<unknown>";

    private final SystemClock clock;
    private final int capacity;
    /** 有界明细队列（队首最旧）；所有访问在实例锁内完成。 */
    private final ArrayDeque<RejectionRecord> records = new ArrayDeque<>();
    private final AtomicLong seq = new AtomicLong();
    /** 全局：每种原因累计被拒条数（只增不减，与明细淘汰无关）。 */
    private final ConcurrentMap<RejectCode, AtomicLong> globalCounts = new ConcurrentHashMap<>();
    /** 按管道：每种原因累计被拒条数（只增不减，与明细淘汰无关）。 */
    private final ConcurrentMap<String, ConcurrentMap<RejectCode, AtomicLong>> perPipelineCounts =
            new ConcurrentHashMap<>();

    public DefaultRejectionLogService(SystemClock clock, ObservabilityProperties properties) {
        this.clock = clock;
        this.capacity = properties.getRejectionLogCapacity();
    }

    @Override
    public RejectionRecord record(String pipelineName, RejectCode code, String message, String rawSnippet) {
        RejectionRecord r = new RejectionRecord();
        r.setPipelineName(pipelineName);
        r.setRejectCode(code);
        r.setMessage(message);
        r.setRawSnippet(rawSnippet == null ? null
                : rawSnippet.substring(0, Math.min(MAX_SNIPPET, rawSnippet.length())));
        r.setRejectedAt(clock.nowMillis());
        synchronized (this) {
            // 序号在临界区内分配：队列顺序与 seq 严格一致，淘汰顺序即到达顺序。
            r.setSeq(seq.incrementAndGet());
            while (records.size() >= capacity) {
                RejectionRecord evicted = records.pollFirst();
                log.info("rejection record evicted by capacity seq={} pipeline={} code={} "
                                + "capacity={} (counters unaffected)",
                        evicted.getSeq(), evicted.getPipelineName(), evicted.getRejectCode(), capacity);
            }
            records.addLast(r);
        }
        // 累计计数与明细存储解耦：一条都不能少、必须始终精确。
        globalCounts.computeIfAbsent(code, k -> new AtomicLong()).incrementAndGet();
        perPipelineCounts
                .computeIfAbsent(pipelineName == null ? UNKNOWN_PIPELINE : pipelineName,
                        k -> new ConcurrentHashMap<>())
                .computeIfAbsent(code, k -> new AtomicLong()).incrementAndGet();
        log.info("rejection recorded seq={} pipeline={} code={} msg={}",
                r.getSeq(), pipelineName, code, message);
        return r;
    }

    @Override
    public List<RejectionRecord> query(String pipelineName, RejectCode code) {
        List<RejectionRecord> snapshot;
        synchronized (this) {
            snapshot = new ArrayList<>(records);
        }
        return snapshot.stream()
                .filter(r -> pipelineName == null || pipelineName.equals(r.getPipelineName()))
                .filter(r -> code == null || code == r.getRejectCode())
                .toList();
    }

    @Override
    public Map<RejectCode, Long> countByCode(String pipelineName) {
        Map<RejectCode, Long> result = new EnumMap<>(RejectCode.class);
        ConcurrentMap<RejectCode, AtomicLong> source = pipelineName == null
                ? globalCounts
                : perPipelineCounts.getOrDefault(pipelineName, new ConcurrentHashMap<>());
        for (RejectCode c : RejectCode.values()) {
            AtomicLong n = source.get(c);
            result.put(c, n == null ? 0L : n.get());
        }
        return result;
    }

    @Override
    public long totalCount(String pipelineName) {
        return countByCode(pipelineName).values().stream().mapToLong(Long::longValue).sum();
    }

    @Override
    public long retainedCount() {
        synchronized (this) {
            return records.size();
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }
}

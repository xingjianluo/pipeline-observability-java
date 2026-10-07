package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
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
 * 进程内留痕实现：明细有容量上限（observability.rejection-log-capacity），
 * 超出后按到达先后淘汰最旧明细，内存占用有界；
 * 按管道/按原因的累计计数用 AtomicLong 独立累加，不随明细淘汰减少，始终精确、不丢不重。
 */
@Service
public class DefaultRejectionLogService implements RejectionLogService {

    private static final Logger log = LoggerFactory.getLogger(DefaultRejectionLogService.class);
    private static final int MAX_SNIPPET = 500;
    /** 无法解析出管道名时的归组键（正常管道名不会与之冲突）。 */
    public static final String UNKNOWN_PIPELINE = "<unknown>";

    private final SystemClock clock;
    private final int capacity;
    /** 留痕明细（有界）：锁内按到达顺序追加/淘汰，查询时先复制再过滤。 */
    private final Deque<RejectionRecord> records = new ArrayDeque<>();
    private final AtomicLong seq = new AtomicLong();
    private final AtomicLong evicted = new AtomicLong();
    /** 全局：每种原因累计被拒条数（只增不减，与明细淘汰无关）。 */
    private final ConcurrentMap<RejectCode, AtomicLong> globalCounts = new ConcurrentHashMap<>();
    /** 按管道：每种原因累计被拒条数（只增不减，与明细淘汰无关）。 */
    private final ConcurrentMap<String, ConcurrentMap<RejectCode, AtomicLong>> perPipelineCounts =
            new ConcurrentHashMap<>();

    public DefaultRejectionLogService(SystemClock clock, ObservabilityProperties properties) {
        this.clock = clock;
        int configured = properties.getRejectionLogCapacity();
        if (configured < 1) {
            log.warn("rejection-log-capacity={} is invalid, falling back to 1", configured);
            configured = 1;
        }
        this.capacity = configured;
    }

    @Override
    public RejectionRecord record(String pipelineName, RejectCode code, String message, String rawSnippet) {
        RejectionRecord r = new RejectionRecord();
        r.setSeq(seq.incrementAndGet());
        r.setPipelineName(pipelineName);
        r.setRejectCode(code);
        r.setMessage(message);
        r.setRawSnippet(rawSnippet == null ? null
                : rawSnippet.substring(0, Math.min(MAX_SNIPPET, rawSnippet.length())));
        r.setRejectedAt(clock.nowMillis());
        synchronized (records) {
            records.addLast(r);
            while (records.size() > capacity) {
                RejectionRecord dropped = records.pollFirst();
                long n = evicted.incrementAndGet();
                log.info("rejection record evicted seq={} pipeline={} code={} (capacity={} evictedTotal={})",
                        dropped.getSeq(), dropped.getPipelineName(), dropped.getRejectCode(),
                        capacity, n);
            }
        }
        // 计数与明细分离：淘汰只影响明细可见范围，累计计数一条不少。
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
        synchronized (records) {
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
    public int capacity() {
        return capacity;
    }

    @Override
    public int retainedCount() {
        synchronized (records) {
            return records.size();
        }
    }

    @Override
    public long evictedCount() {
        return evicted.get();
    }
}

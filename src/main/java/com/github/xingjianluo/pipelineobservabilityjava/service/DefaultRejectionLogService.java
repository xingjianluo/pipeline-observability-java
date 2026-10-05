package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectionRecord;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/** 进程内留痕实现：记录追加有序，计数用 AtomicLong 保证并发精确、不丢不重。 */
@Service
public class DefaultRejectionLogService implements RejectionLogService {

    private static final Logger log = LoggerFactory.getLogger(DefaultRejectionLogService.class);
    private static final int MAX_SNIPPET = 500;
    /** 无法解析出管道名时的归组键（正常管道名不会与之冲突）。 */
    public static final String UNKNOWN_PIPELINE = "<unknown>";

    private final SystemClock clock;
    private final CopyOnWriteArrayList<RejectionRecord> records = new CopyOnWriteArrayList<>();
    private final AtomicLong seq = new AtomicLong();
    /** 全局：每种原因累计被拒条数。 */
    private final ConcurrentMap<RejectCode, AtomicLong> globalCounts = new ConcurrentHashMap<>();
    /** 按管道：每种原因累计被拒条数。 */
    private final ConcurrentMap<String, ConcurrentMap<RejectCode, AtomicLong>> perPipelineCounts =
            new ConcurrentHashMap<>();

    public DefaultRejectionLogService(SystemClock clock) {
        this.clock = clock;
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
        records.add(r);
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
        return records.stream()
                .filter(r -> pipelineName == null || pipelineName.equals(r.getPipelineName()))
                .filter(r -> code == null || code == r.getRejectCode())
                .collect(Collectors.toList());
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
}

package com.github.xingjianluo.pipelineobservabilityjava.store;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectedEvent;

/**
 * 进程内拒绝留痕：明细队列 + 按管道/原因的原子计数。
 * 明细有界（默认 10000 条）防止无限增长，超出后丢弃最早记录；计数永不丢。
 */
@Component
public class InMemoryRejectionLedger implements RejectionLedger {

    static final int DEFAULT_MAX_EVENTS = 10_000;

    private final int maxEvents;
    private final ConcurrentLinkedQueue<RejectedEvent> events = new ConcurrentLinkedQueue<>();
    /** pipelineName(null 用 "|" 表示全量键) -> reason -> 累计计数，全部原子累加。 */
    private final ConcurrentMap<String, ConcurrentMap<RejectCode, AtomicLong>> counters =
            new ConcurrentHashMap<>();
    private final AtomicLong totalSize = new AtomicLong();

    public InMemoryRejectionLedger() {
        this(DEFAULT_MAX_EVENTS);
    }

    InMemoryRejectionLedger(int maxEvents) {
        this.maxEvents = maxEvents;
    }

    private static final String ALL = "\u0000ALL";

    private static String bucket(String pipelineName) {
        return pipelineName == null ? ALL : pipelineName;
    }

    private ConcurrentMap<RejectCode, AtomicLong> countersOf(String key) {
        return counters.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
    }

    private AtomicLong counter(String key, RejectCode code) {
        return countersOf(key).computeIfAbsent(code, c -> new AtomicLong());
    }

    @Override
    public void record(RejectedEvent event) {
        if (event == null || event.getRejectCode() == null) {
            return;
        }
        counter(ALL, event.getRejectCode()).incrementAndGet();
        if (event.getPipelineName() != null) {
            counter(event.getPipelineName(), event.getRejectCode()).incrementAndGet();
        }
        events.add(event);
        // 有界：超出容量时从队首（最早）淘汰，计数不受影响。
        long size = totalSize.incrementAndGet();
        while (size > maxEvents) {
            RejectedEvent removed = events.poll();
            if (removed == null) {
                break;
            }
            size = totalSize.decrementAndGet();
        }
    }

    @Override
    public List<RejectedEvent> query(String pipelineName, RejectCode code) {
        return events.stream()
                .filter(e -> pipelineName == null || pipelineName.equals(e.getPipelineName()))
                .filter(e -> code == null || code == e.getRejectCode())
                .toList();
    }

    @Override
    public Map<RejectCode, Long> countByReason(String pipelineName) {
        // 返回包含全部六类原因的完整映射（未出现的原因为 0），一眼看清各类累计。
        Map<RejectCode, Long> result = new EnumMap<>(RejectCode.class);
        ConcurrentMap<RejectCode, AtomicLong> map = counters.get(bucket(pipelineName));
        for (RejectCode code : RejectCode.values()) {
            AtomicLong n = map == null ? null : map.get(code);
            result.put(code, n == null ? 0L : n.get());
        }
        return result;
    }

    @Override
    public long totalCount(String pipelineName) {
        return countByReason(pipelineName).values().stream().mapToLong(Long::longValue).sum();
    }
}

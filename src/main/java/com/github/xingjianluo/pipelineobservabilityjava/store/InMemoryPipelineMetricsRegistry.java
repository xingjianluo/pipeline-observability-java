package com.github.xingjianluo.pipelineobservabilityjava.store;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Component;

import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineMetrics;

/**
 * 进程内管道计数器：每管道一组原子字段，乱序/重复/迟到/并发下精确累加。
 * 快照在单管道的内置锁内逐字段读取，得到同一时刻、互相能对账的计数。
 */
@Component
public class InMemoryPipelineMetricsRegistry implements PipelineMetricsRegistry {

    /** 无法识别管道名的被拒报文（如畸形 JSON）归入此桶，仍留痕计数，不丢弃。 */
    public static final String UNKNOWN_PIPELINE = "_unknown_";

    private static final class AtomicCounters {
        private final java.util.concurrent.atomic.AtomicLong received = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong accepted = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong duplicate = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong late = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong rejected = new java.util.concurrent.atomic.AtomicLong();

        synchronized PipelineMetrics snapshot(String name) {
            PipelineMetrics m = new PipelineMetrics();
            m.setPipelineName(name);
            m.setReceived(received.get());
            m.setAccepted(accepted.get());
            m.setDuplicate(duplicate.get());
            m.setLate(late.get());
            m.setRejected(rejected.get());
            return m;
        }
    }

    private final ConcurrentMap<String, AtomicCounters> pipelines = new ConcurrentHashMap<>();

    private AtomicCounters of(String pipelineName) {
        String key = pipelineName == null ? UNKNOWN_PIPELINE : pipelineName;
        return pipelines.computeIfAbsent(key, k -> new AtomicCounters());
    }

    @Override
    public void incrementReceived(String pipelineName) {
        of(pipelineName).received.incrementAndGet();
    }

    @Override
    public void incrementAccepted(String pipelineName) {
        of(pipelineName).accepted.incrementAndGet();
    }

    @Override
    public void incrementDuplicate(String pipelineName) {
        of(pipelineName).duplicate.incrementAndGet();
    }

    @Override
    public void incrementLate(String pipelineName) {
        of(pipelineName).late.incrementAndGet();
    }

    @Override
    public void incrementRejected(String pipelineName) {
        of(pipelineName).rejected.incrementAndGet();
    }

    @Override
    public PipelineMetrics snapshot(String pipelineName) {
        AtomicCounters c = pipelines.get(pipelineName == null ? UNKNOWN_PIPELINE : pipelineName);
        if (c == null) {
            PipelineMetrics empty = new PipelineMetrics();
            empty.setPipelineName(pipelineName);
            return empty;
        }
        return c.snapshot(pipelineName);
    }

    @Override
    public List<PipelineMetrics> snapshots() {
        return pipelines.keySet().stream().sorted()
                .map(name -> pipelines.get(name).snapshot(name)).toList();
    }
}

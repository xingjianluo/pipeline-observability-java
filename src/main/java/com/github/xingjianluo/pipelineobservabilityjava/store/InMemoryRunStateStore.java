package com.github.xingjianluo.pipelineobservabilityjava.store;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineRunState;
import org.springframework.stereotype.Component;

/** 基于 ConcurrentHashMap 的进程内实现，mapping 函数内完成状态机收敛。 */
@Component
public class InMemoryRunStateStore implements RunStateStore {

    private final ConcurrentHashMap<String, PipelineRunState> runs = new ConcurrentHashMap<>();

    @Override
    public PipelineRunState compute(String runId, UnaryOperator<PipelineRunState> updater) {
        // compute 对同一 key 原子持锁，乱序/并发事件在 mapping 函数内串行收敛到确定结论。
        return runs.compute(runId, (id, existing) -> {
            PipelineRunState working = existing != null ? existing.copy() : new PipelineRunState(id);
            PipelineRunState result = updater.apply(working);
            // 始终持久化一份独立副本，避免调用方拿到引用后跨锁修改。
            return result.copy();
        }).copy();
    }

    @Override
    public Optional<PipelineRunState> find(String runId) {
        PipelineRunState s = runs.get(runId);
        return Optional.ofNullable(s == null ? null : s.copy());
    }

    @Override
    public List<PipelineRunState> findAll() {
        return runs.values().stream().map(PipelineRunState::copy).toList();
    }
}

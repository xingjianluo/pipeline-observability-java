package com.github.xingjianluo.pipelineobservabilityjava.store;

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineRunState;

/** 运行状态的进程内并发存储：按 runId 收敛，更新原子化。 */
public interface RunStateStore {

    /** 原子地按 runId 取建并更新，返回更新后的状态；并发下每个 run 串行化。 */
    PipelineRunState compute(String runId, UnaryOperator<PipelineRunState> updater);

    Optional<PipelineRunState> find(String runId);

    List<PipelineRunState> findAll();
}

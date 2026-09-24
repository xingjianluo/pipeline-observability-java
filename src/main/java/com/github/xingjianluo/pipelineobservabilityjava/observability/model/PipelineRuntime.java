package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 管道维度的聚合运行时（不可变快照，并发下整体替换）。
 * lastSuccessAt < 0 表示从未成功。
 */
public record PipelineRuntime(
        String pipelineId,
        PipelineConfig config,
        long firstSeenAt,
        long lastEventAt,
        long lastSuccessAt,
        String lastSuccessRunId,
        long lastFailureAt,
        String lastFailureRunId,
        String currentRunId,
        RunStatus currentRunStatus,
        long currentRunStartedAt,
        Long currentRunSlaDeadlineAt
) {
}

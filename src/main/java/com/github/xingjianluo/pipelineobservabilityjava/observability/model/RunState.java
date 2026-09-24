package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 单次运行收敛后的状态快照（不可变）。
 * version 在每次被真正接受并改变状态的事件后 +1，用于并发下的收敛判定。
 */
public record RunState(
        String pipelineId,
        String runId,
        RunStatus status,
        long startedAt,
        long heartbeatAt,
        long lastEventAt,
        Long finishedAt,
        Long scheduledStartAt,
        Long slaDeadlineAt,
        String failMessage,
        int attempts,
        long version
) {
}

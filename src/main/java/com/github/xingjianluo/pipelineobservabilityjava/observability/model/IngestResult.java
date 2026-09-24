package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 事件接入结果。outcome：
 * ACCEPTED 真正推进了状态；DUPLICATE_IGNORED 重复事件；LATE_IGNORED_TERMINAL 迟到且状态已终结；
 * REJECTED 校验拒绝（reason 可区分）。
 */
public record IngestResult(
        String outcome,
        String pipelineId,
        String runId,
        RunStatus runStatus,
        RejectionReason rejectionReason,
        String detail
) {
}

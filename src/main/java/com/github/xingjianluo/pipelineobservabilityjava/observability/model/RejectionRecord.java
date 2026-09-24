package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 被拒绝事件的留痕（可通过接口追溯，绝不静默丢弃）。
 */
public record RejectionRecord(
        long at,
        RejectionReason reason,
        String detail,
        String rawPayload
) {
}

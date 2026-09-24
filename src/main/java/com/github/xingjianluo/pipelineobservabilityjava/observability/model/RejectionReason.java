package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 事件被拒绝的可区分原因（绝不静默丢弃）。
 */
public enum RejectionReason {
    MALFORMED_JSON,
    MISSING_FIELD,
    TYPE_MISMATCH,
    UNSUPPORTED_PROTOCOL_VERSION,
    UNKNOWN_EVENT_TYPE,
    INVALID_FIELD_VALUE
}

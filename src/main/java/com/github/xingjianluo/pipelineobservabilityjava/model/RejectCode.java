package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 事件被拒绝时可区分的原因。 */
public enum RejectCode {
    MISSING_FIELD,
    TYPE_MISMATCH,
    UNSUPPORTED_PROTOCOL_VERSION,
    UNKNOWN_STATUS,
    INVALID_TIMESTAMP
}

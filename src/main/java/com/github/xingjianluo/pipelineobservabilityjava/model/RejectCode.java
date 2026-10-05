package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 事件被拒绝时可区分的原因。 */
public enum RejectCode {
    /** 整条报文格式不合法：非 JSON、畸形 JSON、或 JSON 不是对象。 */
    MALFORMED_JSON,
    MISSING_FIELD,
    TYPE_MISMATCH,
    UNSUPPORTED_PROTOCOL_VERSION,
    UNKNOWN_STATUS,
    INVALID_TIMESTAMP
}

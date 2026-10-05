package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 事件被拒绝时可区分的原因。 */
public enum RejectCode {
    /** 整条报文结构不合法（畸形 JSON / 非 JSON 对象，连一个事件对象都算不上）。 */
    MALFORMED_PAYLOAD,
    /** 必填字段缺失或为空字符串。 */
    MISSING_FIELD,
    /** 字段类型不符（如字符串传了数字、时间传了小数）。 */
    TYPE_MISMATCH,
    /** 协议版本不在允许列表。 */
    UNSUPPORTED_PROTOCOL_VERSION,
    /** status 不是合法枚举值。 */
    UNKNOWN_STATUS,
    /** 时间字段取值不合法（非正整数等）。 */
    INVALID_TIMESTAMP
}

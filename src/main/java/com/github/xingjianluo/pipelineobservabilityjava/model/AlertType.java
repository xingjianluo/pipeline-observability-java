package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 告警类型，与健康判定的异常情形一一对应。 */
public enum AlertType {
    STALE,
    SCHEDULE_LAGGING,
    SLA_BREACHED,
    RUN_FAILED
}

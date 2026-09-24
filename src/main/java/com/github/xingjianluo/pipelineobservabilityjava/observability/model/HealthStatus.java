package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 管道健康判定结论。NEVER_SUCCEEDED/STALE/SCHEDULE_LAGGING/SLA_BREACHED 为告警性结论，
 * TRANSIENT_JITTER 为短暂抖动（不告警），HEALTHY 为正常。
 */
public enum HealthStatus {
    NEVER_SUCCEEDED,
    HEALTHY,
    TRANSIENT_JITTER,
    STALE,
    SCHEDULE_LAGGING,
    SLA_BREACHED
}

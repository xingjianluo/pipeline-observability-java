package com.github.xingjianluo.pipelineobservabilityjava.model;

/**
 * 管道新鲜度/健康判定结论：
 * NEVER_RAN   从未成功过；
 * HEALTHY     正常；
 * STALE       超过新鲜度阈值；
 * SCHEDULE_LAGGING 调度滞后（到点未开始运行）；
 * SLA_BREACHED     SLA 超期（运行超过 SLA 上限）；
 * TRANSIENT_JITTER 短暂抖动（在抖动宽限内）。
 */
public enum FreshnessStatus {
    NEVER_RAN,
    HEALTHY,
    STALE,
    SCHEDULE_LAGGING,
    SLA_BREACHED,
    TRANSIENT_JITTER
}

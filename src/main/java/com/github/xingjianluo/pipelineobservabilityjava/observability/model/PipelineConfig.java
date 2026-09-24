package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 单个管道的生效配置（由全局默认值与管道级覆盖合并而来）。
 *
 * @param neverSucceededGraceMs 首次观测后，多久未成功仍不判 NEVER_SUCCEEDED
 * @param freshnessThresholdMs  距上次成功超过该时长判 STALE（等号不触发，严格大于）
 * @param scheduleIntervalMs    调度周期；<=0 表示不启用调度滞后判定
 * @param scheduleToleranceMs   调度容忍（抖动窗口），实际滞后 > interval+tolerance 才判滞后
 * @param slaRunDurationMs      运行 SLA：运行时长超过该值且未成功判 SLA 违约；<=0 不启用
 * @param jitterGraceMs         短暂抖动宽限：失败后该宽限内仅判 TRANSIENT_JITTER 不告警
 * @param escalationAfterMs     告警持续超过该时长升级 CRITICAL；<=0 不升级
 */
public record PipelineConfig(
        String pipelineId,
        long neverSucceededGraceMs,
        long freshnessThresholdMs,
        long scheduleIntervalMs,
        long scheduleToleranceMs,
        long slaRunDurationMs,
        long jitterGraceMs,
        long escalationAfterMs
) {
}

package com.github.xingjianluo.pipelineobservabilityjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 可配置项（application.properties 前缀 observability）：
 * 协议版本、新鲜度阈值、抖动宽限、SLA 上限、调度间隔、告警升级时长，均支持按管道覆盖。
 */
@ConfigurationProperties(prefix = "observability")
public class ObservabilityProperties {

    /** 支持的协议版本列表，事件版本不在其中则拒绝。 */
    private java.util.List<String> supportedProtocolVersions = new java.util.ArrayList<>();

    /** 默认新鲜度阈值（毫秒）：距上次成功超过该值判为 STALE。 */
    private long defaultFreshnessThresholdMs = 3_600_000L;

    /** 抖动宽限（毫秒）：阈值边界内的短暂超时判为 TRANSIENT_JITTER 而非 STALE。 */
    private long defaultJitterGraceMs = 300_000L;

    /** 默认 SLA 上限（毫秒）：RUNNING 持续时间超过该值判为 SLA_BREACHED。 */
    private long defaultSlaLimitMs = 7_200_000L;

    /** 默认调度间隔（毫秒）：用于推算期望运行时刻，判定 SCHEDULE_LAGGING。 */
    private long defaultScheduleIntervalMs = 3_600_000L;

    /** 调度滞后宽限（毫秒）：超过期望时刻多久才算滞后，避免时钟边界误报。 */
    private long defaultScheduleGraceMs = 120_000L;

    /** 告警升级时长（毫秒）：告警持续超过该值从 WARNING 升级 CRITICAL。 */
    private long escalationAfterMs = 1_800_000L;

    /** 管道级覆盖配置，key 为 pipelineName。 */
    private Map<String, Thresholds> pipelines = new LinkedHashMap<>();

    /** 单管道阈值覆盖项，null 字段表示沿用默认值。 */
    public static class Thresholds {
        private Long freshnessThresholdMs;
        private Long jitterGraceMs;
        private Long slaLimitMs;
        private Long scheduleIntervalMs;
        private Long scheduleGraceMs;

        public Long getFreshnessThresholdMs() { return freshnessThresholdMs; }
        public void setFreshnessThresholdMs(Long freshnessThresholdMs) { this.freshnessThresholdMs = freshnessThresholdMs; }
        public Long getJitterGraceMs() { return jitterGraceMs; }
        public void setJitterGraceMs(Long jitterGraceMs) { this.jitterGraceMs = jitterGraceMs; }
        public Long getSlaLimitMs() { return slaLimitMs; }
        public void setSlaLimitMs(Long slaLimitMs) { this.slaLimitMs = slaLimitMs; }
        public Long getScheduleIntervalMs() { return scheduleIntervalMs; }
        public void setScheduleIntervalMs(Long scheduleIntervalMs) { this.scheduleIntervalMs = scheduleIntervalMs; }
        public Long getScheduleGraceMs() { return scheduleGraceMs; }
        public void setScheduleGraceMs(Long scheduleGraceMs) { this.scheduleGraceMs = scheduleGraceMs; }
    }

    /** 取某管道的有效阈值（覆盖优先，缺省字段回落默认值，保证返回对象字段非空）。 */
    public Thresholds effective(String pipelineName) {
        Thresholds override = pipelineName == null ? null : pipelines.get(pipelineName);
        Thresholds resolved = new Thresholds();
        resolved.setFreshnessThresholdMs(
                override != null && override.getFreshnessThresholdMs() != null
                        ? override.getFreshnessThresholdMs() : defaultFreshnessThresholdMs);
        resolved.setJitterGraceMs(
                override != null && override.getJitterGraceMs() != null
                        ? override.getJitterGraceMs() : defaultJitterGraceMs);
        resolved.setSlaLimitMs(
                override != null && override.getSlaLimitMs() != null
                        ? override.getSlaLimitMs() : defaultSlaLimitMs);
        resolved.setScheduleIntervalMs(
                override != null && override.getScheduleIntervalMs() != null
                        ? override.getScheduleIntervalMs() : defaultScheduleIntervalMs);
        resolved.setScheduleGraceMs(
                override != null && override.getScheduleGraceMs() != null
                        ? override.getScheduleGraceMs() : defaultScheduleGraceMs);
        return resolved;
    }

    /**
     * 该管道是否被显式配置过调度（管道级覆盖中出现任意调度相关键）。
     * 用于区分“按周期调度的管道”与“只看新鲜度的管道”，避免给后者强套周期告警。
     */
    public boolean isScheduleAware(String pipelineName) {
        Thresholds override = pipelineName == null ? null : pipelines.get(pipelineName);
        return override != null
                && (override.getScheduleIntervalMs() != null
                || override.getScheduleGraceMs() != null);
    }

    public java.util.List<String> getSupportedProtocolVersions() { return supportedProtocolVersions; }
    public void setSupportedProtocolVersions(java.util.List<String> supportedProtocolVersions) { this.supportedProtocolVersions = supportedProtocolVersions; }
    public long getDefaultFreshnessThresholdMs() { return defaultFreshnessThresholdMs; }
    public void setDefaultFreshnessThresholdMs(long v) { this.defaultFreshnessThresholdMs = v; }
    public long getDefaultJitterGraceMs() { return defaultJitterGraceMs; }
    public void setDefaultJitterGraceMs(long v) { this.defaultJitterGraceMs = v; }
    public long getDefaultSlaLimitMs() { return defaultSlaLimitMs; }
    public void setDefaultSlaLimitMs(long v) { this.defaultSlaLimitMs = v; }
    public long getDefaultScheduleIntervalMs() { return defaultScheduleIntervalMs; }
    public void setDefaultScheduleIntervalMs(long v) { this.defaultScheduleIntervalMs = v; }
    public long getDefaultScheduleGraceMs() { return defaultScheduleGraceMs; }
    public void setDefaultScheduleGraceMs(long v) { this.defaultScheduleGraceMs = v; }
    public long getEscalationAfterMs() { return escalationAfterMs; }
    public void setEscalationAfterMs(long v) { this.escalationAfterMs = v; }
    public Map<String, Thresholds> getPipelines() { return pipelines; }
    public void setPipelines(Map<String, Thresholds> pipelines) { this.pipelines = pipelines; }
}

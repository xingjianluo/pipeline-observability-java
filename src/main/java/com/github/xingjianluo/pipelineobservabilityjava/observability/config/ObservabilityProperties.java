package com.github.xingjianluo.pipelineobservabilityjava.observability.config;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 可观测性服务的可配置项（前缀 pipeline.observability）。
 * defaults 为全局默认阈值，pipelines 为按管道覆盖的值（仅需给出要覆盖的字段）。
 */
@ConfigurationProperties(prefix = "pipeline.observability")
public class ObservabilityProperties {

    private Defaults defaults = new Defaults();
    private Map<String, Override> pipelines = new LinkedHashMap<>();
    private long evaluationIntervalMs = 5_000L;
    private boolean schedulerEnabled = true;

    public Defaults getDefaults() {
        return defaults;
    }

    public void setDefaults(Defaults defaults) {
        this.defaults = defaults;
    }

    public Map<String, Override> getPipelines() {
        return pipelines;
    }

    public void setPipelines(Map<String, Override> pipelines) {
        this.pipelines = pipelines;
    }

    public long getEvaluationIntervalMs() {
        return evaluationIntervalMs;
    }

    public void setEvaluationIntervalMs(long evaluationIntervalMs) {
        this.evaluationIntervalMs = evaluationIntervalMs;
    }

    public boolean isSchedulerEnabled() {
        return schedulerEnabled;
    }

    public void setSchedulerEnabled(boolean schedulerEnabled) {
        this.schedulerEnabled = schedulerEnabled;
    }

    /** 全局默认阈值。 */
    public static class Defaults {
        private long neverSucceededGraceMs = 60_000L;
        private long freshnessThresholdMs = 300_000L;
        private long scheduleIntervalMs = 0L;
        private long scheduleToleranceMs = 30_000L;
        private long slaRunDurationMs = 0L;
        private long jitterGraceMs = 60_000L;
        private long escalationAfterMs = 900_000L;

        public long getNeverSucceededGraceMs() { return neverSucceededGraceMs; }
        public void setNeverSucceededGraceMs(long v) { this.neverSucceededGraceMs = v; }
        public long getFreshnessThresholdMs() { return freshnessThresholdMs; }
        public void setFreshnessThresholdMs(long v) { this.freshnessThresholdMs = v; }
        public long getScheduleIntervalMs() { return scheduleIntervalMs; }
        public void setScheduleIntervalMs(long v) { this.scheduleIntervalMs = v; }
        public long getScheduleToleranceMs() { return scheduleToleranceMs; }
        public void setScheduleToleranceMs(long v) { this.scheduleToleranceMs = v; }
        public long getSlaRunDurationMs() { return slaRunDurationMs; }
        public void setSlaRunDurationMs(long v) { this.slaRunDurationMs = v; }
        public long getJitterGraceMs() { return jitterGraceMs; }
        public void setJitterGraceMs(long v) { this.jitterGraceMs = v; }
        public long getEscalationAfterMs() { return escalationAfterMs; }
        public void setEscalationAfterMs(long v) { this.escalationAfterMs = v; }
    }

    /**
     * 管道级覆盖：null 字段表示沿用默认值。
     */
    public static class Override {
        private Long neverSucceededGraceMs;
        private Long freshnessThresholdMs;
        private Long scheduleIntervalMs;
        private Long scheduleToleranceMs;
        private Long slaRunDurationMs;
        private Long jitterGraceMs;
        private Long escalationAfterMs;

        public Long getNeverSucceededGraceMs() { return neverSucceededGraceMs; }
        public void setNeverSucceededGraceMs(Long v) { this.neverSucceededGraceMs = v; }
        public Long getFreshnessThresholdMs() { return freshnessThresholdMs; }
        public void setFreshnessThresholdMs(Long v) { this.freshnessThresholdMs = v; }
        public Long getScheduleIntervalMs() { return scheduleIntervalMs; }
        public void setScheduleIntervalMs(Long v) { this.scheduleIntervalMs = v; }
        public Long getScheduleToleranceMs() { return scheduleToleranceMs; }
        public void setScheduleToleranceMs(Long v) { this.scheduleToleranceMs = v; }
        public Long getSlaRunDurationMs() { return slaRunDurationMs; }
        public void setSlaRunDurationMs(Long v) { this.slaRunDurationMs = v; }
        public Long getJitterGraceMs() { return jitterGraceMs; }
        public void setJitterGraceMs(Long v) { this.jitterGraceMs = v; }
        public Long getEscalationAfterMs() { return escalationAfterMs; }
        public void setEscalationAfterMs(Long v) { this.escalationAfterMs = v; }
    }
}

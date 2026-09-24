package com.github.xingjianluo.pipelineobservabilityjava.model;

import java.util.ArrayList;
import java.util.List;

/** 某管道在某个时钟时刻的健康判定结果及可解释依据。 */
public final class HealthAssessment {
    private String pipelineName;
    private FreshnessStatus status;
    private long evaluatedAt;
    private Long lastSuccessTime;
    private Long lastRunStartTime;
    private long freshnessThresholdMs;
    private long jitterGraceMs;
    private long slaLimitMs;
    private Long expectedScheduleMs;
    private Long observedLagMs;
    private Long runningDurationMs;
    private final List<String> reasons = new ArrayList<>();

    public HealthAssessment() {
    }

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public FreshnessStatus getStatus() { return status; }
    public void setStatus(FreshnessStatus status) { this.status = status; }
    public long getEvaluatedAt() { return evaluatedAt; }
    public void setEvaluatedAt(long evaluatedAt) { this.evaluatedAt = evaluatedAt; }
    public Long getLastSuccessTime() { return lastSuccessTime; }
    public void setLastSuccessTime(Long lastSuccessTime) { this.lastSuccessTime = lastSuccessTime; }
    public Long getLastRunStartTime() { return lastRunStartTime; }
    public void setLastRunStartTime(Long lastRunStartTime) { this.lastRunStartTime = lastRunStartTime; }
    public long getFreshnessThresholdMs() { return freshnessThresholdMs; }
    public void setFreshnessThresholdMs(long freshnessThresholdMs) { this.freshnessThresholdMs = freshnessThresholdMs; }
    public long getJitterGraceMs() { return jitterGraceMs; }
    public void setJitterGraceMs(long jitterGraceMs) { this.jitterGraceMs = jitterGraceMs; }
    public long getSlaLimitMs() { return slaLimitMs; }
    public void setSlaLimitMs(long slaLimitMs) { this.slaLimitMs = slaLimitMs; }
    public Long getExpectedScheduleMs() { return expectedScheduleMs; }
    public void setExpectedScheduleMs(Long expectedScheduleMs) { this.expectedScheduleMs = expectedScheduleMs; }
    public Long getObservedLagMs() { return observedLagMs; }
    public void setObservedLagMs(Long observedLagMs) { this.observedLagMs = observedLagMs; }
    public Long getRunningDurationMs() { return runningDurationMs; }
    public void setRunningDurationMs(Long runningDurationMs) { this.runningDurationMs = runningDurationMs; }
    public List<String> getReasons() { return reasons; }
}

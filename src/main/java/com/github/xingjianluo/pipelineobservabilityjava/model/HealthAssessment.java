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
    /** 生效的调度间隔（毫秒），用于推算下一轮本该运行的时刻。 */
    private long scheduleIntervalMs;
    /** 按调度间隔推算出的下一轮本该运行的时刻（epoch 毫秒）。 */
    private Long expectedNextRunMs;
    /** 已连续错过的调度轮数（0 表示未错过）。 */
    private long missedRunCount;
    /** 评估给出的告警级别建议（如连续多轮缺跑时直接 CRITICAL），null 表示按默认 WARNING。 */
    private AlertSeverity suggestedSeverity;
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
    public long getScheduleIntervalMs() { return scheduleIntervalMs; }
    public void setScheduleIntervalMs(long scheduleIntervalMs) { this.scheduleIntervalMs = scheduleIntervalMs; }
    public Long getExpectedNextRunMs() { return expectedNextRunMs; }
    public void setExpectedNextRunMs(Long expectedNextRunMs) { this.expectedNextRunMs = expectedNextRunMs; }
    public long getMissedRunCount() { return missedRunCount; }
    public void setMissedRunCount(long missedRunCount) { this.missedRunCount = missedRunCount; }
    public AlertSeverity getSuggestedSeverity() { return suggestedSeverity; }
    public void setSuggestedSeverity(AlertSeverity suggestedSeverity) { this.suggestedSeverity = suggestedSeverity; }
    public List<String> getReasons() { return reasons; }
}

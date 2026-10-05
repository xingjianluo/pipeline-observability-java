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
    /** 该管道生效的调度周期与宽限（毫秒）。 */
    private long scheduleIntervalMs;
    private long scheduleGraceMs;
    /** 推算出的“下一轮本该什么时候跑”的锚点（上一轮基准）与最近到期轮次时刻。 */
    private Long scheduleAnchorMs;
    private Long nextExpectedRunMs;
    /** 已错过的调度轮次：0=正常/还没到点（含宽限），>=1=错过一轮或连续多轮。 */
    private int missedRounds;
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
    public long getScheduleGraceMs() { return scheduleGraceMs; }
    public void setScheduleGraceMs(long scheduleGraceMs) { this.scheduleGraceMs = scheduleGraceMs; }
    public Long getScheduleAnchorMs() { return scheduleAnchorMs; }
    public void setScheduleAnchorMs(Long scheduleAnchorMs) { this.scheduleAnchorMs = scheduleAnchorMs; }
    public Long getNextExpectedRunMs() { return nextExpectedRunMs; }
    public void setNextExpectedRunMs(Long nextExpectedRunMs) { this.nextExpectedRunMs = nextExpectedRunMs; }
    public int getMissedRounds() { return missedRounds; }
    public void setMissedRounds(int missedRounds) { this.missedRounds = missedRounds; }
    public List<String> getReasons() { return reasons; }
}

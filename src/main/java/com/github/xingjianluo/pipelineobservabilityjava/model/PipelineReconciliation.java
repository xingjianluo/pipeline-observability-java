package com.github.xingjianluo.pipelineobservabilityjava.model;

import java.util.List;
import java.util.Map;

/**
 * 单管道运行质量与告警的对账视图：同一时刻、同一口径的健康结论 + 告警状态 + 各类计数。
 * 健康结论与告警列表来自同一次评估对账（先 assessAndReconcile 再快照），保证互不矛盾。
 */
public final class PipelineReconciliation {
    private String pipelineName;
    private long evaluatedAt;
    /** 当前健康结论（含判定依据 reasons）。 */
    private HealthAssessment health;
    /** 对账后的活跃告警（与 health 同一口径）。 */
    private List<Alert> activeAlerts = List.of();
    /** 接收事件总数（含重复与迟到）。 */
    private long receivedEvents;
    /** 精确重复被合并的事件数。 */
    private long duplicateEvents;
    /** 迟到被忽略的事件数。 */
    private long lateEvents;
    /** 真正推进状态的事件数（received - duplicate - late）。 */
    private long appliedEvents;
    /** 被拒事件总数。 */
    private long rejectedEvents;
    /** 各拒绝原因分别累计被拒条数。 */
    private Map<RejectCode, Long> rejectionsByCode = Map.of();
    /** 告警合并计数：该管道全部告警（含已恢复历史）的合并次数之和。 */
    private long alertMergedCount;
    /** 该管道当前跟踪的运行实例数。 */
    private long runsTracked;

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public long getEvaluatedAt() { return evaluatedAt; }
    public void setEvaluatedAt(long evaluatedAt) { this.evaluatedAt = evaluatedAt; }
    public HealthAssessment getHealth() { return health; }
    public void setHealth(HealthAssessment health) { this.health = health; }
    public List<Alert> getActiveAlerts() { return activeAlerts; }
    public void setActiveAlerts(List<Alert> activeAlerts) { this.activeAlerts = activeAlerts; }
    public long getReceivedEvents() { return receivedEvents; }
    public void setReceivedEvents(long receivedEvents) { this.receivedEvents = receivedEvents; }
    public long getDuplicateEvents() { return duplicateEvents; }
    public void setDuplicateEvents(long duplicateEvents) { this.duplicateEvents = duplicateEvents; }
    public long getLateEvents() { return lateEvents; }
    public void setLateEvents(long lateEvents) { this.lateEvents = lateEvents; }
    public long getAppliedEvents() { return appliedEvents; }
    public void setAppliedEvents(long appliedEvents) { this.appliedEvents = appliedEvents; }
    public long getRejectedEvents() { return rejectedEvents; }
    public void setRejectedEvents(long rejectedEvents) { this.rejectedEvents = rejectedEvents; }
    public Map<RejectCode, Long> getRejectionsByCode() { return rejectionsByCode; }
    public void setRejectionsByCode(Map<RejectCode, Long> rejectionsByCode) { this.rejectionsByCode = rejectionsByCode; }
    public long getAlertMergedCount() { return alertMergedCount; }
    public void setAlertMergedCount(long alertMergedCount) { this.alertMergedCount = alertMergedCount; }
    public long getRunsTracked() { return runsTracked; }
    public void setRunsTracked(long runsTracked) { this.runsTracked = runsTracked; }
}

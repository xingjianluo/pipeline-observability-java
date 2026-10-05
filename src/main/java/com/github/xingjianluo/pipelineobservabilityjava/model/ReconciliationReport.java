package com.github.xingjianluo.pipelineobservabilityjava.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单管道“运行质量 × 上报告警”对账信息：
 * 健康结论与告警列表在同一时刻、同一把锁内取齐，保证口径一致、互不矛盾。
 */
public final class ReconciliationReport {
    private String pipelineName;
    private long evaluatedAt;
    private FreshnessStatus healthStatus;
    private HealthAssessment health;
    private final List<Alert> activeAlerts = new ArrayList<>();
    private PipelineMetrics metrics;
    private final Map<RejectCode, Long> rejectedByReason = new LinkedHashMap<>();
    private long alertMergedCount;
    private long alertSuppressedCount;
    private long alertNotifiedCount;
    /** 健康结论与告警状态是否自洽（true=同一口径，不矛盾）。 */
    private boolean consistent;
    private final List<String> consistencyChecks = new ArrayList<>();

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public long getEvaluatedAt() { return evaluatedAt; }
    public void setEvaluatedAt(long evaluatedAt) { this.evaluatedAt = evaluatedAt; }
    public FreshnessStatus getHealthStatus() { return healthStatus; }
    public void setHealthStatus(FreshnessStatus healthStatus) { this.healthStatus = healthStatus; }
    public HealthAssessment getHealth() { return health; }
    public void setHealth(HealthAssessment health) { this.health = health; }
    public List<Alert> getActiveAlerts() { return activeAlerts; }
    public PipelineMetrics getMetrics() { return metrics; }
    public void setMetrics(PipelineMetrics metrics) { this.metrics = metrics; }
    public Map<RejectCode, Long> getRejectedByReason() { return rejectedByReason; }
    public long getAlertMergedCount() { return alertMergedCount; }
    public void setAlertMergedCount(long alertMergedCount) { this.alertMergedCount = alertMergedCount; }
    public long getAlertSuppressedCount() { return alertSuppressedCount; }
    public void setAlertSuppressedCount(long alertSuppressedCount) { this.alertSuppressedCount = alertSuppressedCount; }
    public long getAlertNotifiedCount() { return alertNotifiedCount; }
    public void setAlertNotifiedCount(long alertNotifiedCount) { this.alertNotifiedCount = alertNotifiedCount; }
    public boolean isConsistent() { return consistent; }
    public void setConsistent(boolean consistent) { this.consistent = consistent; }
    public List<String> getConsistencyChecks() { return consistencyChecks; }
}

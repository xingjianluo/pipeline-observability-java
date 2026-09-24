package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 一条聚合后的告警：同一 (pipeline,type) 在恢复前只保留一条，重复上报合并计数。 */
public final class Alert {
    private String alertId;
    private String pipelineName;
    private AlertType type;
    private AlertState state;
    private AlertSeverity severity;
    private long firstTriggeredAt;
    private long lastTriggeredAt;
    private long count;
    private int escalatedCount;
    private long recoveredAt;
    private String detail;
    private long suppressedCount;
    private long notifiedCount;
    private long version;

    public Alert() {
    }

    public String getAlertId() { return alertId; }
    public void setAlertId(String alertId) { this.alertId = alertId; }
    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public AlertType getType() { return type; }
    public void setType(AlertType type) { this.type = type; }
    public AlertState getState() { return state; }
    public void setState(AlertState state) { this.state = state; }
    public AlertSeverity getSeverity() { return severity; }
    public void setSeverity(AlertSeverity severity) { this.severity = severity; }
    public long getFirstTriggeredAt() { return firstTriggeredAt; }
    public void setFirstTriggeredAt(long firstTriggeredAt) { this.firstTriggeredAt = firstTriggeredAt; }
    public long getLastTriggeredAt() { return lastTriggeredAt; }
    public void setLastTriggeredAt(long lastTriggeredAt) { this.lastTriggeredAt = lastTriggeredAt; }
    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }
    public int getEscalatedCount() { return escalatedCount; }
    public void setEscalatedCount(int escalatedCount) { this.escalatedCount = escalatedCount; }
    public long getRecoveredAt() { return recoveredAt; }
    public void setRecoveredAt(long recoveredAt) { this.recoveredAt = recoveredAt; }
    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
    public long getSuppressedCount() { return suppressedCount; }
    public void setSuppressedCount(long suppressedCount) { this.suppressedCount = suppressedCount; }
    public long getNotifiedCount() { return notifiedCount; }
    public void setNotifiedCount(long notifiedCount) { this.notifiedCount = notifiedCount; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}

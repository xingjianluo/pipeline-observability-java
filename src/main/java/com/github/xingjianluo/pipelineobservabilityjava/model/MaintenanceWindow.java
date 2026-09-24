package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 维护窗口：[startAt, endAt]（epoch 毫秒，闭区间边界），可限定管道（pipelineName 为空表示全局）。 */
public final class MaintenanceWindow {
    private String id;
    private String pipelineName;
    private long startAt;
    private long endAt;
    private String note;

    public MaintenanceWindow() {
    }

    public boolean contains(long epochMillis) {
        return epochMillis >= startAt && epochMillis <= endAt;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public long getStartAt() { return startAt; }
    public void setStartAt(long startAt) { this.startAt = startAt; }
    public long getEndAt() { return endAt; }
    public void setEndAt(long endAt) { this.endAt = endAt; }
    public String getNote() { return note; }
    public void setNote(String note) { this.note = note; }
}

package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 校验通过后的管道运行事件（时间字段统一为 epoch 毫秒）。 */
public final class PipelineEvent {
    private String protocolVersion;
    private String runId;
    private String pipelineName;
    private RunStatus status;
    private long eventTime;
    private Integer attempt;
    private Long scheduledTime;

    public PipelineEvent() {
    }

    public String getProtocolVersion() { return protocolVersion; }
    public void setProtocolVersion(String protocolVersion) { this.protocolVersion = protocolVersion; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public RunStatus getStatus() { return status; }
    public void setStatus(RunStatus status) { this.status = status; }
    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }
    public Integer getAttempt() { return attempt; }
    public void setAttempt(Integer attempt) { this.attempt = attempt; }
    public Long getScheduledTime() { return scheduledTime; }
    public void setScheduledTime(Long scheduledTime) { this.scheduledTime = scheduledTime; }
}

package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 一条被拒上报的留痕记录：可按管道、原因追溯，rawPayload 保留原始报文便于自查。 */
public final class RejectedEvent {
    private long rejectedAt;
    private String pipelineName;
    private String runId;
    private RejectCode rejectCode;
    private String reason;
    private String rawPayload;

    public RejectedEvent() {
    }

    public long getRejectedAt() { return rejectedAt; }
    public void setRejectedAt(long rejectedAt) { this.rejectedAt = rejectedAt; }
    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public RejectCode getRejectCode() { return rejectCode; }
    public void setRejectCode(RejectCode rejectCode) { this.rejectCode = rejectCode; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public String getRawPayload() { return rawPayload; }
    public void setRawPayload(String rawPayload) { this.rawPayload = rawPayload; }
}

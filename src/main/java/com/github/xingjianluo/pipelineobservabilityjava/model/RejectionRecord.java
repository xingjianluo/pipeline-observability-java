package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 一条被拒上报的留痕：谁在什么时候因为什么原因被拒，原文可溯。 */
public final class RejectionRecord {
    private long seq;
    private String pipelineName;
    private RejectCode rejectCode;
    private String message;
    private String rawSnippet;
    private long rejectedAt;

    public RejectionRecord() {
    }

    public long getSeq() { return seq; }
    public void setSeq(long seq) { this.seq = seq; }
    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public RejectCode getRejectCode() { return rejectCode; }
    public void setRejectCode(RejectCode rejectCode) { this.rejectCode = rejectCode; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getRawSnippet() { return rawSnippet; }
    public void setRawSnippet(String rawSnippet) { this.rawSnippet = rawSnippet; }
    public long getRejectedAt() { return rejectedAt; }
    public void setRejectedAt(long rejectedAt) { this.rejectedAt = rejectedAt; }
}

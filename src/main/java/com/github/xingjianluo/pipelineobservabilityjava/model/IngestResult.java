package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 事件接入结果：accepted=false 时 rejectCode/message 必填，便于调用方区分拒绝原因。 */
public final class IngestResult {
    private boolean accepted;
    private boolean duplicate;
    private RejectCode rejectCode;
    private String message;
    private String runId;
    private RunStatus currentStatus;
    private long version;

    public IngestResult() {
    }

    public static IngestResult rejected(RejectCode code, String message) {
        IngestResult r = new IngestResult();
        r.accepted = false;
        r.duplicate = false;
        r.rejectCode = code;
        r.message = message;
        return r;
    }

    public boolean isAccepted() { return accepted; }
    public void setAccepted(boolean accepted) { this.accepted = accepted; }
    public boolean isDuplicate() { return duplicate; }
    public void setDuplicate(boolean duplicate) { this.duplicate = duplicate; }
    public RejectCode getRejectCode() { return rejectCode; }
    public void setRejectCode(RejectCode rejectCode) { this.rejectCode = rejectCode; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public RunStatus getCurrentStatus() { return currentStatus; }
    public void setCurrentStatus(RunStatus currentStatus) { this.currentStatus = currentStatus; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}

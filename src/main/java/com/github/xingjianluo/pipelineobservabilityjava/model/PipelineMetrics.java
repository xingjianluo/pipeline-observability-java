package com.github.xingjianluo.pipelineobservabilityjava.model;

/**
 * 单管道的接入侧计数快照（所有字段在同一把锁内取值，保证对账口径一致）：
 * received  收到的上报总数（接受 + 重复 + 迟到 + 被拒）
 * accepted  被真正应用（推进状态）的去重事件数
 * duplicate 精确重复数
 * late      迟到被忽略数
 * rejected  被拒数
 */
public final class PipelineMetrics {
    private String pipelineName;
    private long received;
    private long accepted;
    private long duplicate;
    private long late;
    private long rejected;

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public long getReceived() { return received; }
    public void setReceived(long received) { this.received = received; }
    public long getAccepted() { return accepted; }
    public void setAccepted(long accepted) { this.accepted = accepted; }
    public long getDuplicate() { return duplicate; }
    public void setDuplicate(long duplicate) { this.duplicate = duplicate; }
    public long getLate() { return late; }
    public void setLate(long late) { this.late = late; }
    public long getRejected() { return rejected; }
    public void setRejected(long rejected) { this.rejected = rejected; }
}

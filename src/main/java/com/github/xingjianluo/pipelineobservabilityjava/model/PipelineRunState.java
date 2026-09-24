package com.github.xingjianluo.pipelineobservabilityjava.model;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** 单个运行（run）的收敛状态，同一 runId 的所有事件汇聚到同一实例。 */
public final class PipelineRunState {
    private final String runId;
    private String pipelineName;
    private RunStatus status;
    private long version;
    private long firstSeenTime;
    private long lastEventTime;
    private Long startTime;
    private Long endTime;
    private Long lastSuccessTime;
    private Long scheduledTimeSeen;
    private long eventCount;
    private long duplicateCount;
    private long lateIgnoredCount;
    private String terminalDetail;
    /** 已处理事件的去重键 (status|eventTime)，精确识别重复上报。 */
    private final Set<String> seenEventKeys = new LinkedHashSet<>();
    private final List<String> transitionLog = new ArrayList<>();

    public PipelineRunState(String runId) {
        this.runId = runId;
    }

    /** 深拷贝（含转移日志），用于并发存储的快照隔离。 */
    public PipelineRunState copy() {
        PipelineRunState c = new PipelineRunState(runId);
        c.pipelineName = pipelineName;
        c.status = status;
        c.version = version;
        c.firstSeenTime = firstSeenTime;
        c.lastEventTime = lastEventTime;
        c.startTime = startTime;
        c.endTime = endTime;
        c.lastSuccessTime = lastSuccessTime;
        c.scheduledTimeSeen = scheduledTimeSeen;
        c.eventCount = eventCount;
        c.duplicateCount = duplicateCount;
        c.lateIgnoredCount = lateIgnoredCount;
        c.terminalDetail = terminalDetail;
        c.seenEventKeys.addAll(seenEventKeys);
        c.transitionLog.addAll(transitionLog);
        return c;
    }

    public String getRunId() { return runId; }
    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public RunStatus getStatus() { return status; }
    public void setStatus(RunStatus status) { this.status = status; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
    public long getFirstSeenTime() { return firstSeenTime; }
    public void setFirstSeenTime(long firstSeenTime) { this.firstSeenTime = firstSeenTime; }
    public long getLastEventTime() { return lastEventTime; }
    public void setLastEventTime(long lastEventTime) { this.lastEventTime = lastEventTime; }
    public Long getStartTime() { return startTime; }
    public void setStartTime(Long startTime) { this.startTime = startTime; }
    public Long getScheduledTimeSeen() { return scheduledTimeSeen; }
    public void setScheduledTimeSeen(Long scheduledTimeSeen) { this.scheduledTimeSeen = scheduledTimeSeen; }
    public Long getEndTime() { return endTime; }
    public void setEndTime(Long endTime) { this.endTime = endTime; }
    public Long getLastSuccessTime() { return lastSuccessTime; }
    public void setLastSuccessTime(Long lastSuccessTime) { this.lastSuccessTime = lastSuccessTime; }
    public long getEventCount() { return eventCount; }
    public void setEventCount(long eventCount) { this.eventCount = eventCount; }
    public long getDuplicateCount() { return duplicateCount; }
    public void setDuplicateCount(long duplicateCount) { this.duplicateCount = duplicateCount; }
    public long getLateIgnoredCount() { return lateIgnoredCount; }
    public void setLateIgnoredCount(long lateIgnoredCount) { this.lateIgnoredCount = lateIgnoredCount; }
    public String getTerminalDetail() { return terminalDetail; }
    public void setTerminalDetail(String terminalDetail) { this.terminalDetail = terminalDetail; }
    public Set<String> getSeenEventKeys() { return seenEventKeys; }
    public List<String> getTransitionLog() { return transitionLog; }
}

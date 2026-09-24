package com.github.xingjianluo.pipelineobservabilityjava.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.xingjianluo.pipelineobservabilityjava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityjava.model.IngestResult;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineEvent;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineRunState;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RunStatus;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;

@Service
public class DefaultEventIngestionService implements EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(DefaultEventIngestionService.class);

    private final RunStateStore store;
    private final ObservabilityProperties properties;
    private final AlertService alertService;
    private final HealthEvaluationService healthEvaluationService;

    public DefaultEventIngestionService(RunStateStore store,
                                        ObservabilityProperties properties,
                                        AlertService alertService,
                                        HealthEvaluationService healthEvaluationService) {
        this.store = store;
        this.properties = properties;
        this.alertService = alertService;
        this.healthEvaluationService = healthEvaluationService;
    }

    @Override
    public IngestResult ingest(JsonNode raw) {
        log.info("ingest event raw={}", raw);
        // 1) 解析 + 严格校验：任何不合规都返回带明确原因的拒绝，绝不静默丢弃。
        PipelineEvent event;
        try {
            event = parse(raw);
        } catch (EventRejectedException e) {
            log.warn("event rejected code={} reason={} raw={}", e.getCode(), e.getMessage(), raw);
            return IngestResult.rejected(e.getCode(), e.getMessage());
        }

        // 2) 在按 runId 串行化的临界区内完成状态机收敛，保证乱序/并发结论一致。
        final PipelineEvent ev = event;
        ConvergenceOutcome outcome = new ConvergenceOutcome();
        PipelineRunState state = store.compute(ev.getRunId(), s -> converge(s, ev, outcome));

        IngestResult r = new IngestResult();
        r.setRunId(ev.getRunId());
        r.setCurrentStatus(state.getStatus());
        r.setVersion(state.getVersion());
        r.setAccepted(true);
        if (outcome.duplicate) {
            r.setDuplicate(true);
            r.setMessage("duplicate event merged, no state change: " + outcome.reason);
        } else if (outcome.lateIgnored) {
            r.setDuplicate(false);
            r.setMessage("late event ignored, terminal/later state preserved: " + outcome.reason);
        } else {
            r.setMessage("applied as " + state.getStatus());
        }
        log.info("converged runId={} status={} version={} duplicate={} reason={}",
                ev.getRunId(), state.getStatus(), state.getVersion(), outcome.duplicate, outcome.reason);
        if (!outcome.duplicate && !outcome.lateIgnored) {
            // 事件真正推进状态后，按当前时钟联动一次健康判定与告警对账（SLA/新鲜度告警随之收敛）。
            long now = System.currentTimeMillis();
            healthEvaluationService.assessAndReconcile(ev.getPipelineName(), now);
        }
        return r;
    }

    private PipelineRunState converge(PipelineRunState s, PipelineEvent ev, ConvergenceOutcome out) {
        long ingestAt = System.currentTimeMillis();
        if (s.getStatus() == null) {
            s.setFirstSeenTime(ingestAt);
        }
        if (s.getPipelineName() == null) {
            s.setPipelineName(ev.getPipelineName());
        }
        s.setEventCount(s.getEventCount() + 1);

        String dedupeKey = ev.getStatus().name() + "|" + ev.getEventTime();
        if (s.getSeenEventKeys().contains(dedupeKey)) {
            // 精确重复：同 (状态,事件时间) 已处理过，只合并计数，状态不动。
            s.setDuplicateCount(s.getDuplicateCount() + 1);
            out.duplicate = true;
            out.reason = "key=" + dedupeKey;
            s.getTransitionLog().add(ingestAt + " DUP " + dedupeKey);
            return s;
        }

        RunStatus current = s.getStatus();
        if (current != null && current.isTerminal()) {
            // 终结态保护：只有“事件时间更新的终结事件”可覆盖（乱序到达的最终结论），
            // 更旧的终结事件或任何非终结事件一律视为迟到，不得回退。
            boolean newerTerminal = ev.getStatus().isTerminal()
                    && s.getEndTime() != null && ev.getEventTime() >= s.getEndTime();
            if (!newerTerminal) {
                s.setLateIgnoredCount(s.getLateIgnoredCount() + 1);
                out.lateIgnored = true;
                out.reason = "terminal=" + current + " @ " + s.getEndTime()
                        + ", incoming=" + ev.getStatus() + " @ " + ev.getEventTime();
                s.getTransitionLog().add(ingestAt + " LATE(terminal-locked) " + out.reason);
                return s;
            }
        } else if (current != null && !ev.getStatus().isTerminal()
                && lifecycleRank(ev.getStatus()) < lifecycleRank(current)) {
            // 非终结期间，生命周期次序倒退（如 RUNNING 后到 PENDING）属于迟到旧状态，不回退。
            s.setLateIgnoredCount(s.getLateIgnoredCount() + 1);
            out.lateIgnored = true;
            out.reason = "lifecycle rollback current=" + current + ", incoming=" + ev.getStatus();
            s.getTransitionLog().add(ingestAt + " LATE(lifecycle-rollback) " + out.reason);
            return s;
        }

        // 接受并应用：终结事件互相覆盖取最新 eventTime（取真实世界最后结论）。
        RunStatus old = current;
        RunStatus next = ev.getStatus();
        s.getSeenEventKeys().add(dedupeKey);
        s.setLastEventTime(Math.max(s.getLastEventTime(), ev.getEventTime()));
        if (ev.getScheduledTime() != null) {
            // 记录看到过的最早调度时间（乱序下取 min）。
            s.setScheduledTimeSeen(s.getScheduledTimeSeen() == null
                    ? ev.getScheduledTime()
                    : Math.min(s.getScheduledTimeSeen(), ev.getScheduledTime()));
        }

        if (next == RunStatus.RUNNING) {
            // 起点取所有 RUNNING 事件中最早的事件时间，乱序到达也不影响 SLA 起算。
            s.setStartTime(s.getStartTime() == null
                    ? ev.getEventTime() : Math.min(s.getStartTime(), ev.getEventTime()));
        }
        if (next.isTerminal()) {
            s.setEndTime(ev.getEventTime());
            if (next == RunStatus.SUCCESS) {
                s.setLastSuccessTime(ev.getEventTime());
            }
        }
        s.setStatus(next);
        s.setVersion(s.getVersion() + 1);
        s.getTransitionLog().add(ingestAt + " APPLY " + old + "->" + s.getStatus()
                + " eventTime=" + ev.getEventTime() + " key=" + dedupeKey);

        // 终结结论切换时联动失败告警（同状态重复的终结覆盖不会重复发通知，告警侧按 key 合并）。
        if (next == RunStatus.FAILED) {
            alertService.reportFailure(s.getPipelineName(), s.getRunId(), ingestAt,
                    "run failed at eventTime=" + ev.getEventTime());
        }
        if (next == RunStatus.SUCCESS && old != RunStatus.SUCCESS) {
            // 失败后更晚的成功到达：立即关闭失败告警，保证“刚刚恢复不残留”。
            alertService.recover(s.getPipelineName(),
                    com.github.xingjianluo.pipelineobservabilityjava.model.AlertType.RUN_FAILED,
                    ingestAt);
        }
        return s;
    }

    private static int lifecycleRank(RunStatus status) {
        return switch (status) {
            case PENDING -> 0;
            case RUNNING -> 1;
            case SUCCESS, FAILED, CANCELLED -> 2;
        };
    }

    private PipelineEvent parse(JsonNode raw) {
        if (raw == null || !raw.isObject()) {
            throw new EventRejectedException(RejectCode.MISSING_FIELD, "request body must be a JSON object");
        }
        PipelineEvent ev = new PipelineEvent();

        JsonNode v = raw.get("protocolVersion");
        if (v == null || v.isNull()) {
            throw new EventRejectedException(RejectCode.MISSING_FIELD, "protocolVersion is required");
        }
        if (!v.isTextual()) {
            throw new EventRejectedException(RejectCode.TYPE_MISMATCH,
                    "protocolVersion must be a string, got " + v.getNodeType());
        }
        if (!properties.getSupportedProtocolVersions().contains(v.asText())) {
            throw new EventRejectedException(RejectCode.UNSUPPORTED_PROTOCOL_VERSION,
                    "protocolVersion '" + v.asText() + "' not supported, allowed="
                            + properties.getSupportedProtocolVersions());
        }
        ev.setProtocolVersion(v.asText());

        ev.setRunId(requireText(raw, "runId"));
        ev.setPipelineName(requireText(raw, "pipelineName"));

        JsonNode st = raw.get("status");
        if (st == null || st.isNull()) {
            throw new EventRejectedException(RejectCode.MISSING_FIELD, "status is required");
        }
        if (!st.isTextual()) {
            throw new EventRejectedException(RejectCode.TYPE_MISMATCH,
                    "status must be a string, got " + st.getNodeType());
        }
        try {
            ev.setStatus(RunStatus.valueOf(st.asText()));
        } catch (IllegalArgumentException e) {
            throw new EventRejectedException(RejectCode.UNKNOWN_STATUS,
                    "unknown status '" + st.asText() + "'");
        }

        ev.setEventTime(requireEpochMillis(raw, "eventTime"));
        if (raw.has("attempt") && !raw.get("attempt").isNull()) {
            JsonNode a = raw.get("attempt");
            if (!a.isIntegralNumber() || a.asLong() < 1) {
                throw new EventRejectedException(RejectCode.TYPE_MISMATCH,
                        "attempt must be a positive integer");
            }
            ev.setAttempt(a.asInt());
        }
        if (raw.has("scheduledTime") && !raw.get("scheduledTime").isNull()) {
            ev.setScheduledTime(requireEpochMillis(raw, "scheduledTime"));
        }
        return ev;
    }

    private String requireText(JsonNode raw, String field) {
        JsonNode n = raw.get(field);
        if (n == null || n.isNull()) {
            throw new EventRejectedException(RejectCode.MISSING_FIELD, field + " is required");
        }
        if (!n.isTextual() || n.asText().isBlank()) {
            throw new EventRejectedException(RejectCode.TYPE_MISMATCH,
                    field + " must be a non-empty string, got " + n.getNodeType());
        }
        return n.asText();
    }

    private long requireEpochMillis(JsonNode raw, String field) {
        JsonNode n = raw.get(field);
        if (n == null || n.isNull()) {
            throw new EventRejectedException(RejectCode.MISSING_FIELD, field + " is required");
        }
        if (!n.isIntegralNumber()) {
            throw new EventRejectedException(RejectCode.TYPE_MISMATCH,
                    field + " must be an epoch-millis integer, got " + n.getNodeType());
        }
        long value = n.asLong();
        if (value <= 0) {
            throw new EventRejectedException(RejectCode.INVALID_TIMESTAMP,
                    field + " must be a positive epoch-millis value, got " + value);
        }
        return value;
    }

    /** 收敛过程结论回传载体（不入存储）。 */
    private static final class ConvergenceOutcome {
        boolean duplicate;
        boolean lateIgnored;
        String reason = "";
    }

    /** 校验失败异常，携带可区分的拒绝码。 */
    private static final class EventRejectedException extends RuntimeException {
        private final RejectCode code;

        EventRejectedException(RejectCode code, String message) {
            super(message);
            this.code = code;
        }

        RejectCode getCode() {
            return code;
        }
    }
}

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
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectedEvent;
import com.github.xingjianluo.pipelineobservabilityjava.model.RunStatus;
import com.github.xingjianluo.pipelineobservabilityjava.store.PipelineMetricsRegistry;
import com.github.xingjianluo.pipelineobservabilityjava.store.RejectionLedger;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

@Service
public class DefaultEventIngestionService implements EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(DefaultEventIngestionService.class);

    private final RunStateStore store;
    private final ObservabilityProperties properties;
    private final AlertService alertService;
    private final HealthEvaluationService healthEvaluationService;
    private final RejectionLedger rejectionLedger;
    private final PipelineMetricsRegistry metricsRegistry;
    private final SystemClock clock;

    public DefaultEventIngestionService(RunStateStore store,
                                        ObservabilityProperties properties,
                                        AlertService alertService,
                                        HealthEvaluationService healthEvaluationService,
                                        RejectionLedger rejectionLedger,
                                        PipelineMetricsRegistry metricsRegistry,
                                        SystemClock clock) {
        this.store = store;
        this.properties = properties;
        this.alertService = alertService;
        this.healthEvaluationService = healthEvaluationService;
        this.rejectionLedger = rejectionLedger;
        this.metricsRegistry = metricsRegistry;
        this.clock = clock;
    }

    @Override
    public IngestResult ingest(JsonNode raw) {
        return ingest(raw, null, null);
    }

    /**
     * @param rawPayload 原始报文字符串（畸形 JSON 无法解析成 JsonNode 时由 Web 层传入，供留痕自查）；
     *                   为 null 时留痕中用 raw 节点的文本形式。
     */
    public IngestResult ingest(JsonNode raw, String rawPayload, RejectCode preclassified) {
        log.info("ingest event raw={}", raw);
        // 1) 解析 + 严格校验：任何不合规都返回带明确原因的拒绝，绝不静默丢弃。
        PipelineEvent event;
        try {
            event = parse(raw, preclassified);
        } catch (EventRejectedException e) {
            String pipelineName = extractPipelineName(raw);
            // 每条被拒上报：留痕（带原始报文）+ 精确计数，可按管道/原因自查。
            recordRejection(pipelineName, extractRunId(raw), e.getCode(), e.getMessage(),
                    rawPayload != null ? rawPayload : String.valueOf(raw));
            metricsRegistry.incrementReceived(pipelineName);
            metricsRegistry.incrementRejected(pipelineName);
            log.warn("event rejected code={} pipeline={} reason={} raw={}",
                    e.getCode(), pipelineName, e.getMessage(), raw);
            return IngestResult.rejected(e.getCode(), e.getMessage());
        }

        // 收到一条可识别管道的上报：先计 received（接受/重复/迟到都恰好计一次）。
        metricsRegistry.incrementReceived(event.getPipelineName());

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
            metricsRegistry.incrementDuplicate(ev.getPipelineName());
        } else if (outcome.lateIgnored) {
            r.setDuplicate(false);
            r.setMessage("late event ignored, terminal/later state preserved: " + outcome.reason);
            metricsRegistry.incrementLate(ev.getPipelineName());
        } else {
            r.setMessage("applied as " + state.getStatus());
            // 只有真正推进状态的事件计 accepted，重复/迟到都不算。
            metricsRegistry.incrementAccepted(ev.getPipelineName());
        }
        log.info("converged runId={} status={} version={} duplicate={} late={} reason={}",
                ev.getRunId(), state.getStatus(), state.getVersion(), outcome.duplicate,
                outcome.lateIgnored, outcome.reason);
        if (!outcome.duplicate && !outcome.lateIgnored) {
            // 事件真正推进状态后联动一次健康判定与告警对账。
            // 评估时刻取“当前时钟”与“本事件时间”的较大者：业务时钟乱序到达时不会让一次旧观测
            // 把告警对账往回拨（与告警侧迟到保护一致），正常推进时即按当前时钟收敛。
            long now = Math.max(clock.nowMillis(), ev.getEventTime());
            healthEvaluationService.assessAndReconcile(ev.getPipelineName(), now);
        }
        return r;
    }

    private void recordRejection(String pipelineName, String runId, RejectCode code,
                                 String reason, String rawPayload) {
        RejectedEvent re = new RejectedEvent();
        re.setRejectedAt(clock.nowMillis());
        re.setPipelineName(pipelineName);
        re.setRunId(runId);
        re.setRejectCode(code);
        re.setReason(reason);
        re.setRawPayload(rawPayload);
        rejectionLedger.record(re);
    }

    /** 在拒绝路径上尽力提取 pipelineName，提取不到（如畸形/非对象）归 null，计数进 unknown 桶。 */
    private static String extractPipelineName(JsonNode raw) {
        try {
            JsonNode n = raw == null ? null : raw.get("pipelineName");
            if (n != null && n.isTextual() && !n.asText().isBlank()) {
                return n.asText();
            }
        } catch (Exception ignore) {
            // 畸形节点不可读时保持 null。
        }
        return null;
    }

    private static String extractRunId(JsonNode raw) {
        try {
            JsonNode n = raw == null ? null : raw.get("runId");
            if (n != null && n.isTextual()) {
                return n.asText();
            }
        } catch (Exception ignore) {
            // 保持 null
        }
        return null;
    }

    private PipelineRunState converge(PipelineRunState s, PipelineEvent ev, ConvergenceOutcome out) {
        long ingestAt = clock.nowMillis();
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

        // 终结结论切换时联动失败告警。告警时刻用事件本身的 eventTime（业务时钟），
        // 这样乱序/并发或固定测试时钟下都满足“旧时刻不能覆盖新状态”的迟到保护。
        if (next == RunStatus.FAILED) {
            alertService.reportFailure(s.getPipelineName(), s.getRunId(), ev.getEventTime(),
                    "run failed at eventTime=" + ev.getEventTime());
        }
        if (next == RunStatus.SUCCESS && old != RunStatus.SUCCESS) {
            // 失败后更晚的成功到达：立即关闭失败告警，保证“刚刚恢复不残留”。
            alertService.recover(s.getPipelineName(),
                    com.github.xingjianluo.pipelineobservabilityjava.model.AlertType.RUN_FAILED,
                    ev.getEventTime());
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

    private PipelineEvent parse(JsonNode raw, RejectCode preclassified) {
        if (preclassified == RejectCode.MALFORMED_PAYLOAD) {
            // 整条报文无法解析（畸形 JSON / 非 JSON 对象）：与“字段类型不对”明确区分。
            throw new EventRejectedException(RejectCode.MALFORMED_PAYLOAD,
                    "malformed or non-JSON request body");
        }
        if (raw == null || !raw.isObject()) {
            throw new EventRejectedException(RejectCode.MALFORMED_PAYLOAD,
                    "request body must be a JSON object");
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

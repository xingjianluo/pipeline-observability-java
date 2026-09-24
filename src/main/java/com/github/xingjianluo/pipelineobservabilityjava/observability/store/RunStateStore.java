package com.github.xingjianluo.pipelineobservabilityjava.observability.store;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.EventType;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineEvent;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RunState;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RunStatus;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 运行状态存储。
 *
 * <p>收敛规则（同一 runId，全部在 {@link ConcurrentHashMap#compute} 的原子块内完成）：</p>
 * <ol>
 *   <li><b>重复</b>：相同事件签名（type|occurredAt|attempt|message）已见过 -> DUPLICATE_IGNORED，
 *       状态不变；</li>
 *   <li><b>终结保护</b>：run 已处于 SUCCEEDED/FAILED/CANCELLED 时，任何新签名的迟到上报 ->
 *       LATE_IGNORED_TERMINAL，终结态绝不回退（先到的 FAILED 与后到的 SUCCEEDED 按先到为准）；</li>
 *   <li><b>乱序</b>：允许迟到事件补齐调度/SLA 字段；lastEventAt 单调取大；</li>
 *   <li><b>并发</b>：compute 对同一 runId 串行化，version 单调递增，结论唯一一致。</li>
 * </ol>
 */
@Component
public class RunStateStore {

    private static final Logger log = LoggerFactory.getLogger(RunStateStore.class);

    /** 接入结论常量。 */
    public static final String ACCEPTED = "ACCEPTED";
    public static final String DUPLICATE_IGNORED = "DUPLICATE_IGNORED";
    public static final String LATE_IGNORED_TERMINAL = "LATE_IGNORED_TERMINAL";

    private record RunEntry(RunState state, Set<String> seenSignatures) {
    }

    private final ConcurrentHashMap<String, RunEntry> runs = new ConcurrentHashMap<>();

    public IngestOutcome apply(PipelineEvent event) {
        final String signature = signature(event);
        final AtomicReference<String> outcomeRef = new AtomicReference<>(ACCEPTED);

        // compute 原子块对同一 runId 串行化，天然收敛乱序与并发
        RunEntry updated = runs.compute(event.runId(), (runId, existing) -> {
            // 1) 重复：完全相同的上报（常见于 producer 重试），直接幂等忽略
            if (existing != null && existing.seenSignatures().contains(signature)) {
                outcomeRef.set(DUPLICATE_IGNORED);
                return existing;
            }

            // 2) 终结保护：已终结 run 上的任何新签名迟到上报都不得回退状态，
            //    但仍登记签名，使同一迟到事件的重发继续被识别为 DUPLICATE。
            if (existing != null && existing.state().status().terminal()) {
                outcomeRef.set(LATE_IGNORED_TERMINAL);
                Set<String> seen = new LinkedHashSet<>(existing.seenSignatures());
                seen.add(signature);
                return new RunEntry(existing.state(), seen);
            }

            // 3) 正常推进（含乱序：迟到但 run 仍在运行 / 首个事件即为终态）
            outcomeRef.set(ACCEPTED);
            Set<String> seen = existing == null
                    ? new LinkedHashSet<>()
                    : new LinkedHashSet<>(existing.seenSignatures());
            seen.add(signature);
            RunState next = transition(existing == null ? null : existing.state(), event);
            return new RunEntry(next, seen);
        });

        String outcome = outcomeRef.get();
        RunState state = updated.state();
        log.info("RunStateStore.apply | runId={} type={} occurredAt={} -> outcome={} status={} version={}",
                event.runId(), event.eventType(), event.occurredAt(), outcome,
                state.status(), state.version());
        return new IngestOutcome(outcome, state);
    }

    /**
     * 纯函数：依据旧状态与事件计算新状态。仅在 run 非终结（或首次出现）时被调用。
     */
    private RunState transition(RunState prev, PipelineEvent e) {
        long startedAt;
        long heartbeatAt;
        long lastEventAt;
        Long finishedAt = null;
        Long scheduledStartAt;
        Long slaDeadlineAt;
        String failMessage = null;
        int attempts;
        RunStatus status;
        long version;

        if (prev == null) {
            startedAt = e.occurredAt();
            heartbeatAt = e.eventType() == EventType.HEARTBEAT ? e.occurredAt() : -1L;
            lastEventAt = e.occurredAt();
            scheduledStartAt = e.scheduledStartAt();
            slaDeadlineAt = e.slaDeadlineAt();
            attempts = e.attempt() != null ? Math.max(1, e.attempt()) : 1;
            status = RunStatus.RUNNING;
            version = 1;
        } else {
            startedAt = prev.startedAt();
            heartbeatAt = prev.heartbeatAt();
            lastEventAt = Math.max(prev.lastEventAt(), e.occurredAt());
            finishedAt = prev.finishedAt();
            scheduledStartAt = e.scheduledStartAt() != null ? e.scheduledStartAt() : prev.scheduledStartAt();
            slaDeadlineAt = e.slaDeadlineAt() != null ? e.slaDeadlineAt() : prev.slaDeadlineAt();
            failMessage = prev.failMessage();
            attempts = prev.attempts();
            status = prev.status();
            version = prev.version();
        }

        switch (e.eventType()) {
            case RUN_STARTED -> startedAt = e.occurredAt();
            case HEARTBEAT -> heartbeatAt = Math.max(heartbeatAt, e.occurredAt());
            case SUCCEEDED -> {
                status = RunStatus.SUCCEEDED;
                finishedAt = e.occurredAt();
            }
            case FAILED -> {
                status = RunStatus.FAILED;
                finishedAt = e.occurredAt();
                failMessage = e.message();
            }
            case CANCELLED -> {
                status = RunStatus.CANCELLED;
                finishedAt = e.occurredAt();
            }
        }

        if (e.attempt() != null) {
            attempts = Math.max(attempts, Math.max(1, e.attempt()));
        }

        return new RunState(e.pipelineId(), e.runId(), status, startedAt, heartbeatAt, lastEventAt,
                finishedAt, scheduledStartAt, slaDeadlineAt, failMessage, attempts, version + 1);
    }

    private static String signature(PipelineEvent e) {
        return e.eventType() + "|" + e.occurredAt() + "|" + (e.attempt() == null ? "" : e.attempt())
                + "|" + (e.message() == null ? "" : e.message());
    }

    public Optional<RunState> get(String runId) {
        RunEntry entry = runs.get(runId);
        return entry == null ? Optional.empty() : Optional.of(entry.state());
    }

    public List<RunState> findByPipeline(String pipelineId) {
        List<RunState> result = new ArrayList<>();
        runs.values().forEach(entry -> {
            if (entry.state().pipelineId().equals(pipelineId)) {
                result.add(entry.state());
            }
        });
        return result;
    }

    /** 事件接入在状态存储侧的收敛结果。 */
    public record IngestOutcome(String outcome, RunState state) {
    }
}

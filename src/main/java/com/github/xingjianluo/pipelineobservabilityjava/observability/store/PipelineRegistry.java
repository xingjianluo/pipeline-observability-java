package com.github.xingjianluo.pipelineobservabilityjava.observability.store;

import com.github.xingjianluo.pipelineobservabilityjava.observability.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineRuntime;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RunState;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RunStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 管道注册表 / 聚合运行时。
 *
 * <ul>
 *   <li>首次上报自动注册，配置 = 全局默认阈值 + 管道级覆盖（null 覆盖项沿用默认）；</li>
 *   <li>{@link #applyRunState} 使用 {@link ConcurrentHashMap#compute} 原子更新，
 *       同一管道的并发上报不会丢失；</li>
 *   <li>lastSuccessAt / lastFailureAt 按 occurredAt 单调取大，迟到终结不会回退已记录的成功/失败时间。</li>
 * </ul>
 */
@Component
public class PipelineRegistry {

    private static final Logger log = LoggerFactory.getLogger(PipelineRegistry.class);

    private final ObservabilityProperties properties;
    private final ConcurrentHashMap<String, PipelineRuntime> pipelines = new ConcurrentHashMap<>();

    public PipelineRegistry(ObservabilityProperties properties) {
        this.properties = properties;
    }

    /** 若管道不存在则按生效配置注册；返回最新聚合态。 */
    public PipelineRuntime ensureRegistered(String pipelineId, long now) {
        return pipelines.computeIfAbsent(pipelineId, id -> {
            PipelineRuntime created = new PipelineRuntime(id, configFor(id), now, -1L, -1L, null,
                    -1L, null, null, null, -1L, null);
            log.info("PipelineRegistry.register | pipelineId={} at={}", id, now);
            return created;
        });
    }

    /**
     * 依据收敛后的 run 状态原子更新管道聚合态。
     * 注意：此处仅在“该 run 比当前记录更新”时推进，防止旧 run 的迟到快照覆盖新 run。
     */
    public void applyRunState(RunState s, long now) {
        ensureRegistered(s.pipelineId(), now);
        pipelines.compute(s.pipelineId(), (id, cur) -> {
            PipelineRuntime prev = cur == null
                    ? new PipelineRuntime(id, configFor(id), now, -1L, -1L, null, -1L, null,
                            null, null, -1L, null)
                    : cur;

            long lastEventAt = Math.max(prev.lastEventAt(), Math.max(s.lastEventAt(), now));
            long lastSuccessAt = prev.lastSuccessAt();
            String lastSuccessRunId = prev.lastSuccessRunId();
            long lastFailureAt = prev.lastFailureAt();
            String lastFailureRunId = prev.lastFailureRunId();
            String currentRunId = prev.currentRunId();
            RunStatus currentRunStatus = prev.currentRunStatus();
            long currentRunStartedAt = prev.currentRunStartedAt();
            Long currentRunSlaDeadlineAt = prev.currentRunSlaDeadlineAt();

            if (s.status() == RunStatus.SUCCEEDED
                    && (s.finishedAt() != null && s.finishedAt() > lastSuccessAt)) {
                lastSuccessAt = s.finishedAt();
                lastSuccessRunId = s.runId();
            }
            if (s.status() == RunStatus.FAILED
                    && (s.finishedAt() != null && s.finishedAt() > lastFailureAt)) {
                lastFailureAt = s.finishedAt();
                lastFailureRunId = s.runId();
            }

            // 当前 run：未记录过、或新 run 开始时间更晚、或同一 run 的新状态，才推进
            boolean newerRun = currentRunId == null
                    || s.startedAt() > currentRunStartedAt
                    || (s.runId().equals(currentRunId));
            if (newerRun) {
                currentRunId = s.runId();
                currentRunStatus = s.status();
                currentRunStartedAt = s.startedAt();
                currentRunSlaDeadlineAt = s.slaDeadlineAt() != null
                        ? s.slaDeadlineAt()
                        : (s.runId().equals(prev.currentRunId()) ? prev.currentRunSlaDeadlineAt() : null);
            }

            PipelineRuntime next = new PipelineRuntime(id, prev.config(), prev.firstSeenAt(),
                    lastEventAt, lastSuccessAt, lastSuccessRunId, lastFailureAt, lastFailureRunId,
                    currentRunId, currentRunStatus, currentRunStartedAt, currentRunSlaDeadlineAt);
            log.info("PipelineRegistry.applyRunState | pipelineId={} runId={} runStatus={} "
                            + "lastSuccessAt={} lastFailureAt={} currentRun={}/{}/{}",
                    id, s.runId(), s.status(), lastSuccessAt, lastFailureAt,
                    currentRunId, currentRunStatus, currentRunStartedAt);
            return next;
        });
    }

    public Optional<PipelineRuntime> get(String pipelineId) {
        return Optional.ofNullable(pipelines.get(pipelineId));
    }

    public List<PipelineRuntime> all() {
        return new ArrayList<>(pipelines.values());
    }

    /** 计算某管道的生效配置：默认值打底，pipeline 级覆盖项（非 null）逐个替换。 */
    public PipelineConfig configFor(String pipelineId) {
        ObservabilityProperties.Defaults d = properties.getDefaults();
        ObservabilityProperties.Override o = properties.getPipelines().get(pipelineId);

        long neverSucceededGraceMs = d.getNeverSucceededGraceMs();
        long freshnessThresholdMs = d.getFreshnessThresholdMs();
        long scheduleIntervalMs = d.getScheduleIntervalMs();
        long scheduleToleranceMs = d.getScheduleToleranceMs();
        long slaRunDurationMs = d.getSlaRunDurationMs();
        long jitterGraceMs = d.getJitterGraceMs();
        long escalationAfterMs = d.getEscalationAfterMs();

        if (o != null) {
            if (o.getNeverSucceededGraceMs() != null) neverSucceededGraceMs = o.getNeverSucceededGraceMs();
            if (o.getFreshnessThresholdMs() != null) freshnessThresholdMs = o.getFreshnessThresholdMs();
            if (o.getScheduleIntervalMs() != null) scheduleIntervalMs = o.getScheduleIntervalMs();
            if (o.getScheduleToleranceMs() != null) scheduleToleranceMs = o.getScheduleToleranceMs();
            if (o.getSlaRunDurationMs() != null) slaRunDurationMs = o.getSlaRunDurationMs();
            if (o.getJitterGraceMs() != null) jitterGraceMs = o.getJitterGraceMs();
            if (o.getEscalationAfterMs() != null) escalationAfterMs = o.getEscalationAfterMs();
        }
        return new PipelineConfig(pipelineId, neverSucceededGraceMs, freshnessThresholdMs,
                scheduleIntervalMs, scheduleToleranceMs, slaRunDurationMs, jitterGraceMs,
                escalationAfterMs);
    }
}

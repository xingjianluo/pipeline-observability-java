package com.github.xingjianluo.pipelineobservabilityjava.observability.service;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthSnapshot;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthStatus;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineRuntime;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RunStatus;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 健康判定（纯函数式，时钟由调用方传入，便于边界测试）。
 *
 * <p>结论优先级：{@code SLA_BREACHED > NEVER_SUCCEEDED > TRANSIENT_JITTER >
 * SCHEDULE_LAGGING > STALE > HEALTHY}。所有阈值均采用<b>严格大于</b>才触发，
 * 恰好等于阈值（时钟边界）不告警；成功后计时自然清零，刚恢复管道不会误报。</p>
 *
 * <ul>
 *   <li>NEVER_SUCCEEDED：从未成功且超过首次观测宽限；宽限内保持 HEALTHY；</li>
 *   <li>STALE：有成功史、未配置调度，距上次成功 &gt; freshnessThresholdMs；</li>
 *   <li>SCHEDULE_LAGGING：配置了周期，距上次成功超过 interval+tolerance；</li>
 *   <li>SLA_BREACHED：当前运行持续时间 &gt; slaRunDurationMs（或事件自带 slaDeadlineAt 已到）仍未成功；</li>
 *   <li>TRANSIENT_JITTER：最近一次失败仍在 jitterGraceMs 内，期间暂缓新鲜度/滞后告警；</li>
 *   <li>HEALTHY：其余情况。</li>
 * </ul>
 */
@Component
public class HealthEvaluator {

    private static final Logger log = LoggerFactory.getLogger(HealthEvaluator.class);

    public HealthSnapshot evaluate(PipelineRuntime r, long now) {
        PipelineConfig cfg = r.config();
        List<String> reasons = new ArrayList<>();
        Set<AlertType> firing = EnumSet.noneOf(AlertType.class);

        boolean neverSucceeded = r.lastSuccessAt() < 0;
        long freshnessAge = neverSucceeded ? -1L : now - r.lastSuccessAt();
        long sinceFirstSeen = now - r.firstSeenAt();

        boolean running = r.currentRunStatus() == RunStatus.RUNNING;
        Long currentRunDurationMs = running ? now - r.currentRunStartedAt() : null;

        // ---------- 1) 运行 SLA（独立判定，优先级最高） ----------
        boolean slaFiring = evaluateSla(r, cfg, now, currentRunDurationMs, reasons);
        if (slaFiring) {
            firing.add(AlertType.SLA_BREACH);
        }

        // ---------- 2) 从未成功 ----------
        boolean neverFiring = false;
        if (!slaFiring && neverSucceeded) {
            if (sinceFirstSeen > cfg.neverSucceededGraceMs()) {
                neverFiring = true;
                firing.add(AlertType.FRESHNESS);
                reasons.add("NEVER_SUCCEEDED: 从未成功 elapsedSinceFirstSeen=" + sinceFirstSeen
                        + "ms > grace=" + cfg.neverSucceededGraceMs()
                        + "ms（严格大于；阈值边界相等不触发）");
            } else {
                reasons.add("从未成功但处于首次观测宽限内 elapsed=" + sinceFirstSeen
                        + "ms <= grace=" + cfg.neverSucceededGraceMs() + "ms => 暂不判定");
            }
        }

        // ---------- 3) 短暂抖动：最近失败仍在宽限内（SLA 为运行中违约，互不冲突） ----------
        boolean jitter = false;
        if (!slaFiring && !neverFiring
                && r.currentRunStatus() == RunStatus.FAILED
                && r.currentRunId() != null
                && r.currentRunId().equals(r.lastFailureRunId())) {
            long failAge = now - r.lastFailureAt();
            if (failAge <= cfg.jitterGraceMs()) {
                jitter = true;
                reasons.add("TRANSIENT_JITTER: run=" + r.currentRunId() + " 失败后 elapsed=" + failAge
                        + "ms <= jitterGrace=" + cfg.jitterGraceMs()
                        + "ms（左闭右含，边界点仍视为抖动），期间新鲜度/滞后告警暂缓");
            } else {
                reasons.add("失败已超过抖动宽限 failAge=" + failAge + "ms > jitterGrace="
                        + cfg.jitterGraceMs() + "ms，按新鲜度/调度规则继续判定");
            }
        }

        // ---------- 4) 调度滞后（配置了周期且有成功史） ----------
        Long scheduleLagMs = null;
        boolean scheduleFiring = false;
        if (!slaFiring && !neverFiring && !jitter && !neverSucceeded
                && cfg.scheduleIntervalMs() > 0) {
            long expectedAt = r.lastSuccessAt() + cfg.scheduleIntervalMs();
            long lag = now - expectedAt;
            scheduleLagMs = Math.max(lag, 0L);
            long allowed = cfg.scheduleToleranceMs();
            if (lag > allowed) {
                scheduleFiring = true;
                firing.add(AlertType.SCHEDULE_LAG);
                reasons.add("SCHEDULE_LAGGING: lag=" + lag + "ms > tolerance=" + allowed
                        + "ms（interval=" + cfg.scheduleIntervalMs()
                        + "ms，严格大于；边界相等不触发）expectedSuccessAt=" + expectedAt);
            } else {
                reasons.add("调度正常: lag=" + lag + "ms <= tolerance=" + allowed
                        + "ms（含边界相等）interval=" + cfg.scheduleIntervalMs() + "ms");
            }
        }

        // ---------- 5) 新鲜度（无周期配置时的超期判定） ----------
        boolean staleFiring = false;
        if (!slaFiring && !neverFiring && !jitter && !scheduleFiring && !neverSucceeded
                && cfg.scheduleIntervalMs() <= 0) {
            if (freshnessAge > cfg.freshnessThresholdMs()) {
                staleFiring = true;
                firing.add(AlertType.FRESHNESS);
                reasons.add("STALE: freshnessAge=" + freshnessAge + "ms > threshold="
                        + cfg.freshnessThresholdMs() + "ms（严格大于；边界相等不触发）"
                        + " lastSuccessAt=" + r.lastSuccessAt() + " run=" + r.lastSuccessRunId());
            } else {
                reasons.add("新鲜度正常: freshnessAge=" + freshnessAge + "ms <= threshold="
                        + cfg.freshnessThresholdMs() + "ms（含边界相等）");
            }
        }

        // 抖动期间显式记录被暂缓的新鲜度事实，保证可解释
        if (jitter && !neverSucceeded) {
            reasons.add("抖动期参考值: freshnessAge=" + freshnessAge + "ms threshold="
                    + cfg.freshnessThresholdMs() + "ms（本次不据此告警）");
        }
        // 刚恢复（当前 run 成功）：明确不误报
        if (r.currentRunStatus() == RunStatus.SUCCEEDED) {
            reasons.add("currentRun=" + r.currentRunId() + " 已成功（刚恢复），新鲜度计时从 lastSuccessAt 重新起算");
        }

        HealthStatus status = resolveStatus(slaFiring, neverFiring, jitter,
                scheduleFiring, staleFiring, running, r);

        HealthSnapshot snapshot = new HealthSnapshot(r.pipelineId(), now, status,
                List.copyOf(reasons), neverSucceeded ? null : freshnessAge, scheduleLagMs,
                currentRunDurationMs, Set.copyOf(firing));
        log.info("HealthEvaluator.evaluate | pipelineId={} at={} status={} firing={} reasons={}",
                r.pipelineId(), now, status, firing, reasons);
        return snapshot;
    }

    private boolean evaluateSla(PipelineRuntime r, PipelineConfig cfg, long now,
                                Long runDuration, List<String> reasons) {
        if (r.currentRunStatus() != RunStatus.RUNNING || runDuration == null) {
            return false;
        }
        // 违约判定两条来源取其一/合并：事件自带 slaDeadlineAt（调度 SLA 截止点），
        // 或配置的最大运行时长 startedAt + slaRunDurationMs。
        Long eventDeadline = r.currentRunSlaDeadlineAt();
        boolean breachByDuration = cfg.slaRunDurationMs() > 0
                && runDuration > cfg.slaRunDurationMs();
        // 事件截止点同样严格大于（now==deadline 时尚未违约）
        boolean breachByDeadline = eventDeadline != null && now > eventDeadline;

        if (breachByDuration) {
            reasons.add("SLA_BREACHED: run=" + r.currentRunId() + " duration=" + runDuration
                    + "ms > slaRunDuration=" + cfg.slaRunDurationMs()
                    + "ms（严格大于；边界相等不触发）startedAt=" + r.currentRunStartedAt()
                    + " configuredDeadline=" + (r.currentRunStartedAt() + cfg.slaRunDurationMs()));
        }
        if (breachByDeadline) {
            reasons.add("SLA_BREACHED: run=" + r.currentRunId() + " now=" + now
                    + " > eventSlaDeadline=" + eventDeadline
                    + "（严格大于；边界相等不触发）");
        }
        if (!breachByDuration && !breachByDeadline
                && (cfg.slaRunDurationMs() > 0 || eventDeadline != null)) {
            reasons.add("SLA 未违约: run=" + r.currentRunId() + " duration=" + runDuration
                    + "ms" + (cfg.slaRunDurationMs() > 0
                            ? " <= slaRunDuration=" + cfg.slaRunDurationMs() + "ms（含边界相等）" : "")
                    + (eventDeadline != null ? " now=" + now + " <= deadline=" + eventDeadline : ""));
        }
        return breachByDuration || breachByDeadline;
    }

    private HealthStatus resolveStatus(boolean sla, boolean never, boolean jitter,
                                       boolean schedule, boolean stale, boolean running,
                                       PipelineRuntime r) {
        if (sla) {
            return HealthStatus.SLA_BREACHED;
        }
        if (never) {
            return HealthStatus.NEVER_SUCCEEDED;
        }
        if (jitter) {
            return HealthStatus.TRANSIENT_JITTER;
        }
        if (schedule) {
            return HealthStatus.SCHEDULE_LAGGING;
        }
        if (stale) {
            return HealthStatus.STALE;
        }
        return HealthStatus.HEALTHY;
    }
}

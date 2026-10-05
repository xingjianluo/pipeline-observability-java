package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.xingjianluo.pipelineobservabilityjava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityjava.config.ObservabilityProperties.Thresholds;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineRunState;
import com.github.xingjianluo.pipelineobservabilityjava.model.RunStatus;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

@Service
public class DefaultHealthEvaluationService implements HealthEvaluationService {

    private static final Logger log = LoggerFactory.getLogger(DefaultHealthEvaluationService.class);

    private final RunStateStore store;
    private final ObservabilityProperties properties;
    private final SystemClock clock;
    private final AlertService alertService;

    public DefaultHealthEvaluationService(RunStateStore store,
                                          ObservabilityProperties properties,
                                          SystemClock clock,
                                          AlertService alertService) {
        this.store = store;
        this.properties = properties;
        this.clock = clock;
        this.alertService = alertService;
    }

    @Override
    public HealthAssessment assessAndReconcile(String pipelineName, long atMillis) {
        HealthAssessment assessment = assess(pipelineName, atMillis);
        alertService.reconcile(assessment, atMillis);
        return assessment;
    }

    @Override
    public HealthAssessment assess(String pipelineName, long atMillis) {
        Thresholds t = properties.effective(pipelineName);
        HealthAssessment a = new HealthAssessment();
        a.setPipelineName(pipelineName);
        a.setEvaluatedAt(atMillis);
        a.setFreshnessThresholdMs(t.getFreshnessThresholdMs());
        a.setJitterGraceMs(t.getJitterGraceMs());
        a.setSlaLimitMs(t.getSlaLimitMs());

        // 聚合该管道的全部运行：取最近成功、最近运行起点、最新一次运行及其调度信息。
        List<PipelineRunState> runs = store.findAll().stream()
                .filter(s -> pipelineName.equals(s.getPipelineName()))
                .toList();
        Long lastSuccess = runs.stream().map(PipelineRunState::getLastSuccessTime)
                .filter(java.util.Objects::nonNull).max(Long::compare).orElse(null);
        PipelineRunState latest = runs.stream()
                .max(Comparator.comparingLong(PipelineRunState::getLastEventTime)).orElse(null);
        Long lastStart = latest != null && latest.getStartTime() != null
                ? latest.getStartTime() : null;
        a.setLastSuccessTime(lastSuccess);
        a.setLastRunStartTime(lastStart);

        FreshnessStatus verdict;
        if (latest == null) {
            // 从未上报过任何运行。
            verdict = FreshnessStatus.NEVER_RAN;
            a.getReasons().add("no run ever reported for pipeline");
        } else if (latest.getStatus() == RunStatus.RUNNING) {
            // 1) 进行中的运行先判 SLA：从实际起点起算，超过 SLA 上限才算超期。
            long duration = atMillis - latest.getStartTime();
            a.setRunningDurationMs(duration);
            if (duration > t.getSlaLimitMs()) {
                verdict = FreshnessStatus.SLA_BREACHED;
                a.getReasons().add("running duration " + duration + "ms exceeds SLA limit "
                        + t.getSlaLimitMs() + "ms (startedAt=" + latest.getStartTime() + ")");
            } else {
                verdict = FreshnessStatus.HEALTHY;
                a.getReasons().add("run is RUNNING within SLA, duration=" + duration
                        + "ms <= slaLimit=" + t.getSlaLimitMs() + "ms");
            }
        } else if (latest.getStatus() == RunStatus.FAILED
                || latest.getStatus() == RunStatus.CANCELLED) {
            // 2) 最近一次运行终结于失败/取消：优先按新鲜度判定（失败本身由 RUN_FAILED 告警承载），
            //    若很快又重跑成功则自然恢复，避免“刚恢复就误报”。
            verdict = assessByFreshness(a, lastSuccess, t, atMillis,
                    "latest run ended " + latest.getStatus() + " @ " + latest.getEndTime());
        } else {
            // 3) PENDING 或最近一次 SUCCESS：判定调度滞后与新鲜度。
            Long scheduled = latest.getScheduledTimeSeen();
            if (scheduled != null && latest.getStatus() != RunStatus.SUCCESS
                    && latest.getStartTime() == null) {
                long lag = atMillis - scheduled;
                a.setExpectedScheduleMs(scheduled);
                a.setObservedLagMs(lag);
                if (lag > t.getScheduleGraceMs()) {
                    verdict = FreshnessStatus.SCHEDULE_LAGGING;
                    a.getReasons().add("scheduled at " + scheduled + ", no start after " + lag
                            + "ms > scheduleGrace " + t.getScheduleGraceMs() + "ms");
                } else {
                    verdict = FreshnessStatus.TRANSIENT_JITTER;
                    a.getReasons().add("schedule lag " + lag + "ms within grace "
                            + t.getScheduleGraceMs() + "ms, treated as transient jitter");
                }
            } else {
                verdict = assessWithSchedule(a, lastSuccess, lastStart, latest, t, atMillis);
            }
        }
        a.setStatus(verdict);
        log.info("assess pipeline={} at={} verdict={} lastSuccess={} threshold={} grace={} reasons={}",
                pipelineName, atMillis, verdict, lastSuccess, t.getFreshnessThresholdMs(),
                t.getJitterGraceMs(), a.getReasons());
        return a;
    }

    /**
     * 新鲜度 + 调度周期联合判定。按配置的调度间隔推算“下一轮本该什么时候跑”：
     * 期望时刻 = max(最近成功, 最近起点, 已见调度时刻) + scheduleIntervalMs。
     * 时钟边界语义（宽限取 scheduleGrace 与 jitterGrace 的较大者，保证与新鲜度抖动带同口径）：
     *   now <= 期望时刻                    => 还没到点，正常
     *   期望时刻 < now <= 期望时刻+宽限     => TRANSIENT_JITTER（正常范围内的延迟，不出正式告警）
     *   now > 期望时刻+宽限                => 已错过调度；错过轮数 = 满足 anchor+k*interval+grace < now 的 k 个数
     * 错过轮数 >= criticalMissedRuns 时建议告警级别直接 CRITICAL（错过越多越严重）。
     * 新鲜度已 STALE 时以 STALE 为准（缺轮信息仍记录在字段与 reasons 中）。
     */
    private FreshnessStatus assessWithSchedule(HealthAssessment a, Long lastSuccess, Long lastStart,
                                               PipelineRunState latest, Thresholds t, long atMillis) {
        FreshnessStatus freshness = assessByFreshness(a, lastSuccess, t, atMillis, null);
        a.setScheduleIntervalMs(t.getScheduleIntervalMs());

        Long anchor = max(lastSuccess, lastStart, latest.getScheduledTimeSeen());
        if (anchor == null) {
            a.getReasons().add("no schedule anchor (no success/start/scheduled time), "
                    + "schedule-based check skipped");
            return freshness;
        }
        long interval = t.getScheduleIntervalMs();
        long grace = Math.max(t.getScheduleGraceMs(), t.getJitterGraceMs());
        long expectedNext = anchor + interval;
        long lateness = atMillis - expectedNext;
        a.setExpectedNextRunMs(expectedNext);
        a.setObservedLagMs(lateness);

        long missed = 0;
        if (lateness > grace) {
            // 错过轮数：第 k 轮在 anchor+k*interval+grace 仍未运行即计为错过（边界含等号不误报）。
            missed = (lateness - grace - 1) / interval + 1;
        }
        a.setMissedRunCount(missed);
        a.getReasons().add("schedule: anchor=" + anchor + " interval=" + interval
                + "ms expectedNext=" + expectedNext + " lateness=" + lateness
                + "ms grace=" + grace + "ms missedRuns=" + missed);

        if (freshness == FreshnessStatus.STALE) {
            a.getReasons().add("freshness STALE takes precedence over schedule-miss verdict");
            return freshness;
        }
        if (missed >= 1) {
            if (missed >= properties.getCriticalMissedRuns()) {
                a.setSuggestedSeverity(com.github.xingjianluo.pipelineobservabilityjava
                        .model.AlertSeverity.CRITICAL);
                a.getReasons().add("missed " + missed + " consecutive scheduled runs >= criticalMissedRuns "
                        + properties.getCriticalMissedRuns() + ", severity suggested CRITICAL");
            } else {
                a.getReasons().add("missed " + missed + " scheduled run(s), severity WARNING");
            }
            return FreshnessStatus.SCHEDULE_LAGGING;
        }
        if (lateness > 0) {
            a.getReasons().add("past expected next run by " + lateness
                    + "ms, within grace, treated as transient jitter (no formal alert)");
            return FreshnessStatus.TRANSIENT_JITTER;
        }
        a.getReasons().add("next scheduled run not due yet (due at " + expectedNext + ")");
        return freshness;
    }

    private static Long max(Long... values) {
        Long result = null;
        for (Long v : values) {
            if (v != null && (result == null || v > result)) {
                result = v;
            }
        }
        return result;
    }

    /**
     * 新鲜度判定，时钟边界语义：
     * age == threshold          => HEALTHY（阈值为“超过才报”，等号不报）
     * threshold < age <= +grace => TRANSIENT_JITTER（短暂抖动，不产生正式告警）
     * age > threshold + grace   => STALE
     * 从未成功                   => NEVER_RAN（即使有失败运行，也与“正常后超期”区分开）
     */
    private FreshnessStatus assessByFreshness(HealthAssessment a, Long lastSuccess,
                                              Thresholds t, long atMillis, String extra) {
        if (extra != null) {
            a.getReasons().add(extra);
        }
        if (lastSuccess == null) {
            FreshnessStatus v = FreshnessStatus.NEVER_RAN;
            a.getReasons().add("no successful run ever; distinct from a healthy pipeline going stale");
            return v;
        }
        long age = atMillis - lastSuccess;
        long threshold = t.getFreshnessThresholdMs();
        long grace = t.getJitterGraceMs();
        if (age <= threshold) {
            a.getReasons().add("fresh: age " + age + "ms <= threshold " + threshold
                    + "ms (boundary inclusive, no false alarm)");
            return FreshnessStatus.HEALTHY;
        }
        if (age <= threshold + grace) {
            a.getReasons().add("transient jitter: age " + age + "ms within threshold " + threshold
                    + "ms + grace " + grace + "ms");
            return FreshnessStatus.TRANSIENT_JITTER;
        }
        a.getReasons().add("stale: age " + age + "ms > threshold " + threshold
                + "ms + grace " + grace + "ms");
        return FreshnessStatus.STALE;
    }

    @Override
    public List<HealthAssessment> assessAll(long atMillis) {
        // 管道名集合来自已有运行（pipelineName 在首个事件时落库）。
        Map<String, Boolean> names = new LinkedHashMap<>();
        for (PipelineRunState s : store.findAll()) {
            if (s.getPipelineName() != null) {
                names.putIfAbsent(s.getPipelineName(), Boolean.TRUE);
            }
        }
        return names.keySet().stream().sorted().map(n -> assess(n, atMillis)).toList();
    }
}

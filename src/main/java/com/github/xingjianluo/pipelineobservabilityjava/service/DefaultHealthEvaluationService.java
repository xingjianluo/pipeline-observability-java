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
        a.setScheduleIntervalMs(t.getScheduleIntervalMs());
        a.setScheduleGraceMs(t.getScheduleGraceMs());

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
            //    正在跑本身就证明调度被满足，任何错过轮次告警随即解除。
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
        } else if (latest.getStatus() == RunStatus.PENDING
                && latest.getStartTime() == null
                && latest.getScheduledTimeSeen() != null) {
            // 3) 最近一轮已到调度、但运行还没开始：针对“这一轮”的 scheduledTime 判滞后。
            verdict = assessPendingSchedule(a, latest.getScheduledTimeSeen(), t, atMillis);
        } else {
            // 4) 最近一次 SUCCESS（或无调度信息的旧态）：按配置的调度周期推算“下一轮本该何时跑”。
            //    只有显式配置过调度的管道才走周期判定，其余仍按新鲜度，避免给非周期管道误报。
            Long anchor = scheduleAnchor(latest, lastSuccess);
            boolean scheduleAware = properties.isScheduleAware(pipelineName);
            if (scheduleAware && anchor != null && t.getScheduleIntervalMs() > 0) {
                verdict = assessByScheduleInterval(a, anchor, lastSuccess, t, atMillis);
            } else {
                verdict = assessByFreshness(a, lastSuccess, t, atMillis, null);
            }
        }
        a.setStatus(verdict);
        log.info("assess pipeline={} at={} verdict={} lastSuccess={} interval={} grace={} missed={} reasons={}",
                pipelineName, atMillis, verdict, lastSuccess, t.getScheduleIntervalMs(),
                t.getScheduleGraceMs(), a.getMissedRounds(), a.getReasons());
        return a;
    }

    /**
     * 最近一轮处于 PENDING、尚未开始：以该轮 scheduledTime 为基准。
     * lag = now - scheduled。
     *   lag <= grace                 => TRANSIENT_JITTER（还没点或宽限内，正常延迟，不告警）
     *   grace < lag <= interval+grace=> SCHEDULE_LAGGING，错过 1 轮
     *   再往后每过一个 interval 多错过一轮。
     * 边界含等号：lag==grace 不告警。
     */
    private FreshnessStatus assessPendingSchedule(HealthAssessment a, long scheduled,
                                                  Thresholds t, long atMillis) {
        long interval = t.getScheduleIntervalMs();
        long grace = t.getScheduleGraceMs();
        long lag = atMillis - scheduled;
        a.setExpectedScheduleMs(scheduled);
        a.setScheduleAnchorMs(scheduled);
        a.setObservedLagMs(lag);
        if (lag <= grace) {
            a.setMissedRounds(0);
            a.getReasons().add("scheduled at " + scheduled + ", lag " + lag
                    + "ms within grace " + grace + "ms: not due yet / transient jitter, no alert");
            return FreshnessStatus.TRANSIENT_JITTER;
        }
        int missed = missedRounds(lag, interval, grace);
        a.setMissedRounds(missed);
        a.getReasons().add("pending run scheduled at " + scheduled + " never started, lag " + lag
                + "ms > grace " + grace + "ms, missedRounds=" + missed + " (interval=" + interval + "ms)");
        return FreshnessStatus.SCHEDULE_LAGGING;
    }

    /**
     * 按调度周期推算下一轮：
     *   nextDue = anchor + interval（anchor 为最近一次调度基准：
     *   优先取看到的最早 scheduledTime，其次最近成功时间，再次最近运行结束时间）。
     * lag = now - nextDue。
     *   lag <= 0                    => HEALTHY（两轮之间还没到点）
     *   0 < lag <= grace            => TRANSIENT_JITTER（宽限内的正常延迟，不正式告警）
     *   lag > grace                 => SCHEDULE_LAGGING；
     *                                   missedRounds = floor((lag - grace - 1)/interval) + 1
     * 边界：lag==0、lag==grace 都不告警；连续错过的轮次越多 missedRounds 越大，结论越严重。
     */
    private FreshnessStatus assessByScheduleInterval(HealthAssessment a, long anchor, Long lastSuccess,
                                                     Thresholds t, long atMillis) {
        long interval = t.getScheduleIntervalMs();
        long grace = t.getScheduleGraceMs();
        long nextDue = anchor + interval;
        long lag = atMillis - nextDue;
        a.setScheduleAnchorMs(anchor);
        a.setNextExpectedRunMs(nextDue);
        a.setObservedLagMs(lag);
        if (lag <= 0) {
            a.setMissedRounds(0);
            a.getReasons().add("between rounds: next run due at " + nextDue + ", not due yet"
                    + " (lag " + lag + "ms), HEALTHY; interval=" + interval + "ms anchor=" + anchor);
            return FreshnessStatus.HEALTHY;
        }
        if (lag <= grace) {
            a.setMissedRounds(0);
            a.getReasons().add("due at " + nextDue + ", lag " + lag + "ms within grace " + grace
                    + "ms: transient jitter, no formal alert");
            return FreshnessStatus.TRANSIENT_JITTER;
        }
        int missed = missedRounds(lag, interval, grace);
        a.setMissedRounds(missed);
        a.getReasons().add("missed " + missed + " scheduled round(s): due at " + nextDue
                + ", lag " + lag + "ms > grace " + grace + "ms, interval=" + interval
                + "ms, anchor=" + anchor + ", lastSuccess=" + lastSuccess);
        return FreshnessStatus.SCHEDULE_LAGGING;
    }

    /** 错过轮次：超过宽限后，第 1 个 interval 为错过 1 轮，每多跨一个整周期加 1 轮。 */
    private static int missedRounds(long lag, long interval, long grace) {
        if (lag <= grace || interval <= 0) {
            return 0;
        }
        return (int) ((lag - grace - 1) / interval + 1);
    }

    /** 调度基准：优先最近一次已知 scheduledTime，其次最近成功时间，再次最近运行结束时间。 */
    private static Long scheduleAnchor(PipelineRunState latest, Long lastSuccess) {
        if (latest.getScheduledTimeSeen() != null) {
            return latest.getScheduledTimeSeen();
        }
        if (lastSuccess != null) {
            return lastSuccess;
        }
        return latest.getEndTime();
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

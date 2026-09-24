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
                verdict = assessByFreshness(a, lastSuccess, t, atMillis, null);
            }
        }
        a.setStatus(verdict);
        log.info("assess pipeline={} at={} verdict={} lastSuccess={} threshold={} grace={} reasons={}",
                pipelineName, atMillis, verdict, lastSuccess, t.getFreshnessThresholdMs(),
                t.getJitterGraceMs(), a.getReasons());
        return a;
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

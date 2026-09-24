package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthSnapshot;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthStatus;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineRuntime;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RunStatus;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.HealthEvaluator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 健康判定边界测试：阈值严格大于、首次宽限、短暂抖动、调度滞后容忍、SLA 边界与刚恢复不误报。
 */
class HealthEvaluatorTest {

    private static final Logger log = LoggerFactory.getLogger(HealthEvaluatorTest.class);

    private final HealthEvaluator evaluator = new HealthEvaluator();

    /** 无调度周期的默认测试配置：新鲜度 300s，首次宽限 60s，抖动宽限 60s，运行 SLA 120s。 */
    private PipelineConfig cfg(long freshnessMs, long intervalMs, long toleranceMs,
                              long slaMs, long jitterMs, long graceMs) {
        return new PipelineConfig("p", graceMs, freshnessMs, intervalMs, toleranceMs, slaMs,
                jitterMs, 900_000L);
    }

    private PipelineRuntime runtime(PipelineConfig c, long firstSeen, long lastSuccess,
                                    String successRun, long lastFail, String failRun,
                                    String currentRun, RunStatus currentStatus,
                                    long currentStarted, Long slaDeadline) {
        return new PipelineRuntime("p", c, firstSeen, Math.max(lastSuccess, lastFail),
                lastSuccess, successRun, lastFail, failRun, currentRun, currentStatus,
                currentStarted, slaDeadline);
    }

    @Test
    void neverSucceededRespectsInitialGrace() {
        PipelineConfig c = cfg(300_000, 0, 0, 0, 60_000, 60_000);
        HealthSnapshot inGrace = evaluator.evaluate(
                runtime(c, 0, -1, null, -1, null, "r1", RunStatus.RUNNING, 0, null), 60_000);
        HealthSnapshot overdue = evaluator.evaluate(
                runtime(c, 0, -1, null, -1, null, "r1", RunStatus.RUNNING, 0, null), 60_001);
        log.info("[从未成功] at=60000(grace边界, 相等不触发) => {} ; at=60001(严格大于) => {} reasons={}",
                inGrace.status(), overdue.status(), overdue.reasons());
        assertEquals(HealthStatus.HEALTHY, inGrace.status());
        assertEquals(HealthStatus.NEVER_SUCCEEDED, overdue.status());
        assertTrue(overdue.firingAlertTypes().contains(AlertType.FRESHNESS));
    }

    @Test
    void freshnessBoundaryIsStrictlyGreater() {
        PipelineConfig c = cfg(300_000, 0, 0, 0, 60_000, 60_000);
        PipelineRuntime r = runtime(c, 0, 1_000, "r1", -1, null,
                "r1", RunStatus.SUCCEEDED, 0, null);
        HealthSnapshot atBoundary = evaluator.evaluate(r, 301_000);
        HealthSnapshot overBoundary = evaluator.evaluate(r, 301_001);
        log.info("[新鲜度边界] age=300000=阈值 => {} ; age=300001>阈值 => {}",
                atBoundary.status(), overBoundary.status());
        assertEquals(HealthStatus.HEALTHY, atBoundary.status());
        assertEquals(HealthStatus.STALE, overBoundary.status());
    }

    @Test
    void scheduleLagBoundaryAndJustRecovered() {
        PipelineConfig c = cfg(300_000, 60_000, 30_000, 0, 60_000, 60_000);
        PipelineRuntime lagging = runtime(c, 0, 0, "r1", -1, null,
                "r1", RunStatus.SUCCEEDED, 0, null);
        // expectedAt=60000, lag>30000 才触发：at=90000 边界相等不触发；90001 触发
        HealthSnapshot boundary = evaluator.evaluate(lagging, 90_000);
        HealthSnapshot firing = evaluator.evaluate(lagging, 90_001);
        log.info("[调度滞后] at=90000(lag=tolerance=30000, 相等) => {} ; at=90001 => {}",
                boundary.status(), firing.status());
        assertEquals(HealthStatus.HEALTHY, boundary.status());
        assertEquals(HealthStatus.SCHEDULE_LAGGING, firing.status());

        // 刚刚恢复：新 run 成功，lastSuccessAt 推进，立即回到 HEALTHY（不残留误报）
        PipelineRuntime recovered = runtime(c, 0, 90_001, "r2", 50_000, "rX",
                "r2", RunStatus.SUCCEEDED, 80_000, null);
        HealthSnapshot nowHealthy = evaluator.evaluate(recovered, 90_001);
        log.info("[刚恢复] lastSuccessAt 推进至 90001 => {} reasons={}",
                nowHealthy.status(), nowHealthy.status());
        assertEquals(HealthStatus.HEALTHY, nowHealthy.status());
    }

    @Test
    void transientJitterSuppressesFreshness() {
        PipelineConfig c = cfg(300_000, 0, 0, 0, 60_000, 60_000);
        // 上次成功在 1000，当前 run 于 400000 失败：age=400000 本已 STALE，但失败才 1s，判抖动
        PipelineRuntime jittering = runtime(c, 0, 1_000, "r1", 400_000, "r2",
                "r2", RunStatus.FAILED, 399_000, null);
        HealthSnapshot jitter = evaluator.evaluate(jittering, 401_000);
        // 超过抖动宽限后，新鲜度超期重新生效
        HealthSnapshot settled = evaluator.evaluate(jittering, 460_001);
        log.info("[抖动] failAge=1000<=60000 => {} firing={} ; failAge=60001 => {}",
                jitter.status(), jitter.firingAlertTypes(), settled.status());
        assertEquals(HealthStatus.TRANSIENT_JITTER, jitter.status());
        assertTrue(jitter.firingAlertTypes().isEmpty(), "抖动宽限内不得发新鲜度告警");
        assertEquals(HealthStatus.STALE, settled.status());
    }

    @Test
    void slaBoundaryByDurationAndEventDeadline() {
        PipelineConfig c = cfg(300_000, 0, 0, 120_000, 60_000, 60_000);
        PipelineRuntime running = runtime(c, 0, 1_000, "r1", -1, null,
                "r2", RunStatus.RUNNING, 0, null);
        HealthSnapshot atBoundary = evaluator.evaluate(running, 120_000);
        HealthSnapshot over = evaluator.evaluate(running, 120_001);
        log.info("[运行SLA] duration=120000=阈值 => {} ; duration=120001 => {}",
                atBoundary.status(), over.status());
        assertEquals(HealthStatus.HEALTHY, atBoundary.status());
        assertEquals(HealthStatus.SLA_BREACHED, over.status());

        // 事件自带截止点：now==deadline 不违约，严格大于才违约
        PipelineRuntime withDeadline = runtime(c, 0, 1_000, "r1", -1, null,
                "r3", RunStatus.RUNNING, 0, 50_000L);
        assertEquals(HealthStatus.HEALTHY, evaluator.evaluate(withDeadline, 50_000).status());
        HealthSnapshot deadlineBreach = evaluator.evaluate(withDeadline, 50_001);
        log.info("[事件SLA截止] now=50001 > deadline=50000 => {} reasons={}",
                deadlineBreach.status(), deadlineBreach.reasons());
        assertEquals(HealthStatus.SLA_BREACHED, deadlineBreach.status());
        assertTrue(deadlineBreach.firingAlertTypes().contains(AlertType.SLA_BREACH));
    }
}

package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertSeverity;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.service.AlertService;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.HealthEvaluationService;

/**
 * 调度周期判定边界（显式配置 interval/grace 的管道）：
 * - 还没到点（两轮之间）=> HEALTHY，不错报；
 * - 正好踩在下一轮边界 / 宽限边界 => 不告警；
 * - 宽限外错过 1 轮 => SCHEDULE_LAGGING/WARNING；
 * - 连续错过多轮 => missedRounds 递增且升级 CRITICAL；
 * - 刚补跑 => 立即恢复；调大/调小周期，结论跟随变化。
 * 每个用例使用独立管道，避免共享上下文里的状态互相干扰（与既有测试一致）。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
@TestPropertySource(properties = {
        "observability.pipelines.s-b1.schedule-interval-ms=600000",
        "observability.pipelines.s-b1.schedule-grace-ms=30000",
        "observability.pipelines.s-m1.schedule-interval-ms=600000",
        "observability.pipelines.s-m1.schedule-grace-ms=30000",
        "observability.pipelines.s-c1.schedule-interval-ms=600000",
        "observability.pipelines.s-c1.schedule-grace-ms=30000",
        "observability.pipelines.s-fast.schedule-interval-ms=600000",
        "observability.pipelines.s-fast.schedule-grace-ms=30000",
        "observability.pipelines.s-slow.schedule-interval-ms=3600000",
        "observability.pipelines.s-slow.schedule-grace-ms=120000",
        "observability.pipelines.s-pd.schedule-interval-ms=600000",
        "observability.pipelines.s-pd.schedule-grace-ms=30000"
})
class ScheduleMissBoundaryTest {

    private static final Logger log = LoggerFactory.getLogger(ScheduleMissBoundaryTest.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private static final long T0 = ClockTestConfig.T0;

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private HealthEvaluationService health;
    @Autowired
    private AlertService alerts;

    private void event(String pipeline, String runId, String status, long t, Long scheduled) {
        String sched = scheduled == null ? "" : ",\"scheduledTime\":" + scheduled;
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\""
                + pipeline + "\",\"status\":\"" + status + "\",\"eventTime\":" + t + sched + "}";
        try {
            ingestion.ingest(mapper.readTree(json));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private HealthAssessment check(String pipeline, long at, FreshnessStatus expected, int missed) {
        HealthAssessment a = health.assess(pipeline, at);
        log.info("[CHECK] pipeline={} at={} verdict={} expected={} missed={} expectedMissed={} nextDue={} reasons={}",
                pipeline, at, a.getStatus(), expected, a.getMissedRounds(), missed,
                a.getNextExpectedRunMs(), a.getReasons());
        assertEquals(expected, a.getStatus());
        assertEquals(missed, a.getMissedRounds(), "missedRounds mismatch at " + at);
        return a;
    }

    @Test
    void betweenRoundsAndBoundariesDoNotFalseAlarm() {
        String p = "s-b1";
        event(p, "b-run1", "SUCCESS", T0, null);
        // 下一轮应在 T0+600s。还没到点（lag<0）=> HEALTHY
        check(p, T0 + 599_999L, FreshnessStatus.HEALTHY, 0);
        // 正好踩在周期边界 lag==0 => 仍 HEALTHY（边界含等号，不误报）
        check(p, T0 + 600_000L, FreshnessStatus.HEALTHY, 0);
        // 刚过点 1ms，但宽限内 => TRANSIENT_JITTER，不正式告警
        check(p, T0 + 600_001L, FreshnessStatus.TRANSIENT_JITTER, 0);
        // 正好宽限边界 lag==grace(30s) => 仍抖动，不告警
        check(p, T0 + 630_000L, FreshnessStatus.TRANSIENT_JITTER, 0);
        // 宽限外 1ms => 错过 1 轮
        check(p, T0 + 630_001L, FreshnessStatus.SCHEDULE_LAGGING, 1);
    }

    @Test
    void missedRoundsAccumulateAndEscalateSeverity() {
        String p = "s-m1";
        event(p, "m-run1", "SUCCESS", T0, null);
        long due = T0 + 600_000L;

        // 错过恰好 2 轮边界：lag = grace + interval 时仍 1 轮；再多 1ms => 2 轮
        check(p, due + 30_001L, FreshnessStatus.SCHEDULE_LAGGING, 1);
        check(p, due + 630_000L, FreshnessStatus.SCHEDULE_LAGGING, 1);
        check(p, due + 630_001L, FreshnessStatus.SCHEDULE_LAGGING, 2);
        // 连续错过 3 轮
        check(p, due + 1_230_001L, FreshnessStatus.SCHEDULE_LAGGING, 3);

        // 联动告警：1 轮时 WARNING
        health.assessAndReconcile(p, due + 30_001L);
        Alert one = alerts.list(false).stream()
                .filter(a -> a.getType() == AlertType.SCHEDULE_LAGGING).findFirst().orElseThrow();
        log.info("[ALERT 1 ROUND] severity={} count={}", one.getSeverity(), one.getCount());
        assertEquals(AlertSeverity.WARNING, one.getSeverity());

        // 观测结论加重到 3 轮 => CRITICAL（同一条告警，只升不降）
        health.assessAndReconcile(p, due + 1_230_001L);
        Alert three = alerts.list(false).stream()
                .filter(a -> a.getType() == AlertType.SCHEDULE_LAGGING).findFirst().orElseThrow();
        log.info("[ALERT 3 ROUNDS] severity={} count={}", three.getSeverity(), three.getCount());
        assertEquals(AlertSeverity.CRITICAL, three.getSeverity());
    }

    @Test
    void catchUpRunRecoversImmediately() {
        String p = "s-c1";
        event(p, "c-run1", "SUCCESS", T0, null);
        long due = T0 + 600_000L;
        check(p, due + 300_000L, FreshnessStatus.SCHEDULE_LAGGING, 1);
        health.assessAndReconcile(p, due + 300_000L);
        assertEquals(1, alerts.list(false).stream()
                .filter(a -> a.getPipelineName().equals(p)
                        && a.getType() == AlertType.SCHEDULE_LAGGING).count());

        // 刚补跑起来（RUNNING）：调度被满足，下一次判定立即恢复，告警同步解除
        event(p, "c-run2", "RUNNING", due + 301_000L, null);
        health.assessAndReconcile(p, due + 301_001L);
        check(p, due + 301_001L, FreshnessStatus.HEALTHY, 0);
        long left = alerts.list(false).stream()
                .filter(a -> a.getPipelineName().equals(p)
                        && a.getType() == AlertType.SCHEDULE_LAGGING).count();
        log.info("[RECOVERED] lagging alerts left={}", left);
        assertEquals(0, left, "catch-up run must clear schedule lagging alert immediately");
    }

    @Test
    void changingIntervalChangesVerdict() {
        // 同一时间点：10 分钟周期已错过；1 小时周期还没到点。
        event("s-fast", "i-fast", "SUCCESS", T0, null);
        event("s-slow", "i-slow", "SUCCESS", T0, null);
        long at = T0 + 900_000L; // 15 分钟后
        // 10 分钟周期：due=t+600s，lag=300s > grace 30s，已错过 1 轮
        check("s-fast", at, FreshnessStatus.SCHEDULE_LAGGING, 1);
        // 1 小时周期：due=t+3600s，lag<0 => HEALTHY
        check("s-slow", at, FreshnessStatus.HEALTHY, 0);
    }

    @Test
    void pendingScheduledRunUsesItsOwnScheduleBoundary() {
        String p = "s-pd";
        long scheduled = T0;
        // 最近一轮 PENDING、尚未开始，scheduledTime 为 scheduled
        event(p, "pd-run", "PENDING", scheduled - 1000L, scheduled);
        // lag==grace => 抖动，不告警
        check(p, scheduled + 30_000L, FreshnessStatus.TRANSIENT_JITTER, 0);
        // lag>grace => 错过 1 轮
        check(p, scheduled + 30_001L, FreshnessStatus.SCHEDULE_LAGGING, 1);
        // 又过一个完整周期 => 错过 2 轮
        check(p, scheduled + 630_001L, FreshnessStatus.SCHEDULE_LAGGING, 2);
    }
}

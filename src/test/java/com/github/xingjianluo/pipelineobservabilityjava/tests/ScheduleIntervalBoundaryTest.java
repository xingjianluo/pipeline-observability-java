package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertSeverity;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertState;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.service.AlertService;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.HealthEvaluationService;

/**
 * 调度周期判定：按配置的 scheduleIntervalMs 推算下一轮本该运行的时刻。
 * p-int10/p-int10b 周期 10min、宽限 1min；p-int30 周期 30min、宽限 1min；
 * 新鲜度阈值拉到 24h，保证结论完全由调度周期驱动。
 * 事件时间取真实当前时刻为基准，避免接入时的联动对账用真实时钟打出干扰告警。
 */
@SpringBootTest(properties = {
        "observability.pipelines.p-int10.schedule-interval-ms=600000",
        "observability.pipelines.p-int10.schedule-grace-ms=60000",
        "observability.pipelines.p-int10.jitter-grace-ms=60000",
        "observability.pipelines.p-int10.freshness-threshold-ms=86400000",
        "observability.pipelines.p-int10b.schedule-interval-ms=600000",
        "observability.pipelines.p-int10b.schedule-grace-ms=60000",
        "observability.pipelines.p-int10b.jitter-grace-ms=60000",
        "observability.pipelines.p-int10b.freshness-threshold-ms=86400000",
        "observability.pipelines.p-int30.schedule-interval-ms=1800000",
        "observability.pipelines.p-int30.schedule-grace-ms=60000",
        "observability.pipelines.p-int30.jitter-grace-ms=60000",
        "observability.pipelines.p-int30.freshness-threshold-ms=86400000"
})
@Import(ClockTestConfig.class)
class ScheduleIntervalBoundaryTest {

    private static final Logger log = LoggerFactory.getLogger(ScheduleIntervalBoundaryTest.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private HealthEvaluationService health;
    @Autowired
    private AlertService alerts;

    private void success(String pipeline, String runId, long t) throws Exception {
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\""
                + pipeline + "\",\"status\":\"SUCCESS\",\"eventTime\":" + t + "}";
        log.info("[INPUT] {}", json);
        ingestion.ingest(mapper.readTree(json));
    }

    private HealthAssessment check(String pipeline, long at, FreshnessStatus expected, long missed) {
        HealthAssessment a = health.assess(pipeline, at);
        log.info("[CHECK] pipeline={} at={} verdict={} expected={} expectedNext={} lateness={} "
                        + "missed={} severity={} reasons={}",
                pipeline, at, a.getStatus(), expected, a.getExpectedNextRunMs(),
                a.getObservedLagMs(), a.getMissedRunCount(), a.getSuggestedSeverity(), a.getReasons());
        assertEquals(expected, a.getStatus());
        assertEquals(missed, a.getMissedRunCount());
        return a;
    }

    @Test
    void verdictFollowsConfiguredIntervalWithExactBoundaries() throws Exception {
        long base = System.currentTimeMillis();
        log.info("[INPUT] base={} interval(p-int10)=600s grace=60s interval(p-int30)=1800s", base);
        success("p-int10", "si-1", base);
        success("p-int30", "si-2", base);

        // 正好踩在周期边界：now == anchor+interval，还没到点，正常
        HealthAssessment a = check("p-int10", base + 600_000L, FreshnessStatus.HEALTHY, 0);
        assertEquals(base + 600_000L, a.getExpectedNextRunMs().longValue());
        // 过点但在宽限内（含等号）：正常范围内的延迟，TRANSIENT_JITTER，不出正式告警
        check("p-int10", base + 660_000L, FreshnessStatus.TRANSIENT_JITTER, 0);
        // 宽限边界 +1ms：错过第 1 轮
        check("p-int10", base + 660_001L, FreshnessStatus.SCHEDULE_LAGGING, 1);
        // 第二轮的宽限边界上（含等号）：仍只算错过 1 轮，不误报第 2 轮
        check("p-int10", base + 1_260_000L, FreshnessStatus.SCHEDULE_LAGGING, 1);
        // 连续错过 2 轮：结论更严重（建议 CRITICAL）
        HealthAssessment severe = check("p-int10", base + 1_260_001L, FreshnessStatus.SCHEDULE_LAGGING, 2);
        assertEquals(AlertSeverity.CRITICAL, severe.getSuggestedSeverity());

        // 同一时刻、同一事件历史，调度周期配置不同结论不同：30min 周期此时还没到点
        check("p-int30", base + 660_001L, FreshnessStatus.HEALTHY, 0);
        // 30min 周期错过 1 轮
        check("p-int30", base + 1_860_001L, FreshnessStatus.SCHEDULE_LAGGING, 1);
    }

    @Test
    void missedRunsEscalateAlertAndCatchUpRecoversImmediately() throws Exception {
        long base = System.currentTimeMillis();
        success("p-int10b", "si-3", base);

        // 错过 1 轮：SCHEDULE_LAGGING 告警触发，WARNING
        health.assessAndReconcile("p-int10b", base + 660_001L);
        Alert firing = activeAlert("p-int10b");
        log.info("[ALERT] missed=1 state={} severity={}", firing.getState(), firing.getSeverity());
        assertEquals(AlertType.SCHEDULE_LAGGING, firing.getType());
        assertEquals(AlertState.FIRING, firing.getState());
        assertEquals(AlertSeverity.WARNING, firing.getSeverity());

        // 连续错过 2 轮：同一条告警升级为 CRITICAL（错过越多越严重）
        health.assessAndReconcile("p-int10b", base + 1_260_001L);
        Alert critical = activeAlert("p-int10b");
        log.info("[ALERT] missed=2 state={} severity={} escalatedCount={}",
                critical.getState(), critical.getSeverity(), critical.getEscalatedCount());
        assertEquals(AlertSeverity.CRITICAL, critical.getSeverity());

        // 刚刚补跑成功：立即恢复健康，告警同口径关闭、无残留
        success("p-int10b", "si-4", base + 1_300_000L);
        HealthAssessment a = health.assessAndReconcile("p-int10b", base + 1_300_001L);
        log.info("[RECOVERED] verdict={} reasons={}", a.getStatus(), a.getReasons());
        assertEquals(FreshnessStatus.HEALTHY, a.getStatus());
        assertEquals(0, alerts.list(false).stream()
                .filter(x -> "p-int10b".equals(x.getPipelineName())).count(),
                "no active alert may remain right after catch-up");
        Alert recovered = alerts.list(true).stream()
                .filter(x -> "p-int10b".equals(x.getPipelineName())
                        && x.getState() == AlertState.RECOVERED)
                .findFirst().orElseThrow();
        assertEquals(AlertType.SCHEDULE_LAGGING, recovered.getType());
    }

    private Alert activeAlert(String pipeline) {
        List<Alert> actives = alerts.list(false).stream()
                .filter(x -> pipeline.equals(x.getPipelineName())).toList();
        assertEquals(1, actives.size(), "expect exactly one active alert for " + pipeline);
        return actives.get(0);
    }
}

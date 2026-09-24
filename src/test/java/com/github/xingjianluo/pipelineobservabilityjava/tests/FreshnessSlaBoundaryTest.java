package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.HealthEvaluationService;

/**
 * 时钟边界（阈值来自 application.properties：freshness=3600s, jitter=300s,
 * sla=7200s, scheduleGrace=120s）：等号不报、刚恢复不误报、短暂抖动不升级为 STALE。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
class FreshnessSlaBoundaryTest {

    private static final Logger log = LoggerFactory.getLogger(FreshnessSlaBoundaryTest.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private HealthEvaluationService health;

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

    private HealthAssessment check(String pipeline, long at, FreshnessStatus expected) {
        HealthAssessment a = health.assess(pipeline, at);
        log.info("[CHECK] pipeline={} at={} verdict={} expected={} reasons={}",
                pipeline, at, a.getStatus(), expected, a.getReasons());
        assertEquals(expected, a.getStatus());
        return a;
    }

    @Test
    void neverRanIsDistinctFromStale() {
        check("p-never", ClockTestConfig.T0, FreshnessStatus.NEVER_RAN);
    }

    @Test
    void freshnessBoundaryIsInclusiveAndJitterBand() {
        long successAt = ClockTestConfig.T0;
        event("p-fresh", "run-f", "SUCCESS", successAt, null);
        // age == threshold(3600s)：边界含等号，仍 HEALTHY
        check("p-fresh", successAt + 3_600_000L, FreshnessStatus.HEALTHY);
        // threshold+1ms：进入抖动带，TRANSIENT_JITTER，不报 STALE
        check("p-fresh", successAt + 3_600_001L, FreshnessStatus.TRANSIENT_JITTER);
        // threshold+grace：抖动带上界仍为抖动
        check("p-fresh", successAt + 3_900_000L, FreshnessStatus.TRANSIENT_JITTER);
        // 超出阈值+宽限：STALE
        check("p-fresh", successAt + 3_900_001L, FreshnessStatus.STALE);
    }

    @Test
    void justRecoveredPipelineDoesNotFalseAlarm() {
        long t0 = ClockTestConfig.T0;
        event("p-recover", "run-r1", "FAILED", t0, null);
        // 失败后不久（旧成功时间在阈值内由新成功刷新）：先给一次更早成功
        event("p-recover", "run-r0", "SUCCESS", t0 - 1_000_000L, null);
        check("p-recover", t0 + 1000, FreshnessStatus.HEALTHY);
        // 紧接着重跑成功：lastSuccess 刷新到 t0+2s，绝不误报
        event("p-recover", "run-r2", "SUCCESS", t0 + 2_000L, null);
        check("p-recover", t0 + 2_000L, FreshnessStatus.HEALTHY);
        check("p-recover", t0 + 2_000L + 3_600_000L, FreshnessStatus.HEALTHY);
    }

    @Test
    void slaBoundaryAndBreachForRunning() {
        long start = ClockTestConfig.T0;
        event("p-sla", "run-s", "RUNNING", start, null);
        // duration == sla(7200s)：未超过 => 不超期
        check("p-sla", start + 7_200_000L, FreshnessStatus.HEALTHY);
        // 超过 1ms => SLA_BREACHED
        check("p-sla", start + 7_200_001L, FreshnessStatus.SLA_BREACHED);
        // 运行成功结束后，按新鲜度（lastSuccess=endTime）判定，SLA 告警自然恢复
        event("p-sla", "run-s", "SUCCESS", start + 7_200_005L, null);
        check("p-sla", start + 7_200_005L, FreshnessStatus.HEALTHY);
    }

    @Test
    void scheduleLagUsesGraceBoundary() {
        long scheduled = ClockTestConfig.T0;
        // PENDING 且带 scheduledTime，尚无 RUNNING 起点
        event("p-sched", "run-p", "PENDING", scheduled - 1000L, scheduled);
        // lag == grace(120s)：宽限含等号 => 抖动，不判滞后
        check("p-sched", scheduled + 120_000L, FreshnessStatus.TRANSIENT_JITTER);
        // lag > grace => SCHEDULE_LAGGING
        check("p-sched", scheduled + 120_001L, FreshnessStatus.SCHEDULE_LAGGING);
        // 运行开始后，调度滞后解除
        event("p-sched", "run-p", "RUNNING", scheduled + 130_000L, scheduled);
        check("p-sched", scheduled + 130_001L, FreshnessStatus.HEALTHY);
    }
}

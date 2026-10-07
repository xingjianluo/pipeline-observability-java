package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertState;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineReconciliation;
import com.github.xingjianluo.pipelineobservabilityjava.service.AlertService;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.HealthEvaluationService;
import com.github.xingjianluo.pipelineobservabilityjava.service.InMemoryNotificationSink;
import com.github.xingjianluo.pipelineobservabilityjava.service.ReconciliationService;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/**
 * 固定服务时间下的“结论与告警同口径”回归：
 * - 上报联动、主动查询、对账三条路径在同一时刻给出同一结论；
 * - 恢复正常后告警当场恢复：活跃列表无残留、历史有 RECOVERED 记录、
 *   RECOVERY 通知恰好一次、重复恢复不重复通知；
 * - 结论落到 TRANSIENT_JITTER（不出正式告警）时，已挂的健康类告警同样当场恢复。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
class AlertRecoveryConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(AlertRecoveryConsistencyTest.class);
    private static final long T0 = ClockTestConfig.T0;
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private HealthEvaluationService health;
    @Autowired
    private AlertService alerts;
    @Autowired
    private ReconciliationService reconciliation;
    @Autowired
    private InMemoryNotificationSink sink;
    @Autowired
    private SystemClock clock;

    private void send(String pipeline, String runId, String status, long eventTime) throws Exception {
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId
                + "\",\"pipelineName\":\"" + pipeline + "\",\"status\":\"" + status
                + "\",\"eventTime\":" + eventTime + "}";
        log.info("[INPUT] {}", json);
        var r = ingestion.ingest(mapper.readTree(json));
        log.info("[INGESTED] accepted={} dup={} msg={}", r.isAccepted(), r.isDuplicate(), r.getMessage());
    }

    private long activeCount(String pipeline) {
        return alerts.list(false).stream().filter(a -> pipeline.equals(a.getPipelineName())).count();
    }

    private long notificationCount(String pipeline, String kind) {
        return sink.notifications().stream()
                .filter(n -> n.pipeline().equals(pipeline) && n.kind().equals(kind)).count();
    }

    @Test
    void staleAlertRecoversAndAllPathsAgreeUnderFixedClock() throws Exception {
        String p = "fix-recover";
        ((MutableClock) clock).set(T0);
        log.info("[CLOCK] fixed at T0={} (freshnessThreshold=3600s jitterGrace=300s)", T0);

        // 1) 制造超期：lastSuccess=T0-4.9M，对 T0 而言 age=4.9M > 3.6M+0.3M -> STALE
        send(p, "fr-1", "RUNNING", T0 - 5_000_000L);
        send(p, "fr-1", "SUCCESS", T0 - 4_900_000L);

        // 2) 三条路径同一时刻必须同口径：联动后立刻查询与对账，结论都是 STALE、告警都在挂
        HealthAssessment query = health.assess(p, clock.nowMillis());
        PipelineReconciliation recon = reconciliation.reconcile(p);
        log.info("[AGREE-STALE] queryVerdict={} reconVerdict={} activeAlerts={} consistent={} notes={}",
                query.getStatus(), recon.getHealth().getStatus(),
                recon.getActiveAlerts().stream().map(a -> a.getType().name()).toList(),
                recon.isConsistent(), recon.getConsistencyNotes());
        assertEquals(FreshnessStatus.STALE, query.getStatus());
        assertEquals(FreshnessStatus.STALE, recon.getHealth().getStatus());
        assertEquals(1, activeCount(p), "STALE verdict must come with exactly one active STALE alert");
        assertTrue(recon.isConsistent(), "reconciliation must be self-consistent");
        assertEquals(1, notificationCount(p, "FIRING"), "STALE alert fired exactly once");

        // 3) 管道恢复正常：补跑成功（eventTime=T0），时钟不动
        send(p, "fr-2", "SUCCESS", T0);

        // 4) 同口径恢复：结论 HEALTHY，告警当场恢复——活跃无残留、历史有 RECOVERED、通知恰好一条
        HealthAssessment queryAfter = health.assess(p, clock.nowMillis());
        PipelineReconciliation reconAfter = reconciliation.reconcile(p);
        long recoveredInHistory = alerts.list(true).stream()
                .filter(a -> p.equals(a.getPipelineName()) && a.getState() == AlertState.RECOVERED)
                .count();
        log.info("[RECOVERED] queryVerdict={} reconVerdict={} activeLeft={} recoveredHistory={} "
                        + "recoveryNotes={} consistent={}",
                queryAfter.getStatus(), reconAfter.getHealth().getStatus(), activeCount(p),
                recoveredInHistory, notificationCount(p, "RECOVERY"), reconAfter.isConsistent());
        assertEquals(FreshnessStatus.HEALTHY, queryAfter.getStatus());
        assertEquals(FreshnessStatus.HEALTHY, reconAfter.getHealth().getStatus());
        assertEquals(0, activeCount(p), "HEALTHY verdict must not leave any active alert behind");
        assertEquals(1, recoveredInHistory, "recovered alert must be traceable in history");
        assertEquals(1, notificationCount(p, "RECOVERY"), "exactly one RECOVERY notification");
        assertTrue(reconAfter.isConsistent());

        // 5) 重复恢复是空操作：不重复发通知、不产生新的历史记录
        assertNull(alerts.recover(p, AlertType.STALE, clock.nowMillis()));
        assertEquals(1, notificationCount(p, "RECOVERY"), "idempotent recovery, no duplicate notification");
        assertEquals(1, alerts.list(true).stream()
                .filter(a -> p.equals(a.getPipelineName()) && a.getState() == AlertState.RECOVERED)
                .count());
        log.info("[IDEMPOTENT] duplicate recovery is a no-op, notifications unchanged");
    }

    @Test
    void transientJitterVerdictAlsoClearsHealthAlerts() throws Exception {
        String p = "fix-jitter";
        ((MutableClock) clock).set(T0);

        // 1) 先进入 STALE：lastSuccess=T0-4.9M
        send(p, "fj-1", "SUCCESS", T0 - 4_900_000L);
        assertEquals(FreshnessStatus.STALE, health.assess(p, clock.nowMillis()).getStatus());
        assertEquals(1, activeCount(p));
        log.info("[STALE-FIRST] verdict=STALE activeAlerts={}", activeCount(p));

        // 2) 新成功把 age 拉回到抖动带 (3.6M, 3.9M]：lastSuccess=T0-3.8M -> age=3.8M
        //    结论为 TRANSIENT_JITTER（不出正式告警），已挂的 STALE 告警必须当场恢复
        send(p, "fj-2", "SUCCESS", T0 - 3_800_000L);
        HealthAssessment query = health.assess(p, clock.nowMillis());
        PipelineReconciliation recon = reconciliation.reconcile(p);
        log.info("[JITTER] queryVerdict={} reconVerdict={} activeLeft={} consistent={} notes={}",
                query.getStatus(), recon.getHealth().getStatus(), activeCount(p),
                recon.isConsistent(), recon.getConsistencyNotes());
        assertEquals(FreshnessStatus.TRANSIENT_JITTER, query.getStatus());
        assertEquals(FreshnessStatus.TRANSIENT_JITTER, recon.getHealth().getStatus());
        assertEquals(0, activeCount(p),
                "TRANSIENT_JITTER produces no formal alert, so none may stay active");
        assertEquals(1, notificationCount(p, "RECOVERY"));
        assertTrue(recon.isConsistent());
    }
}

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
 * 固定服务时间下的“结论与告警同口径”：
 * 上报联动、主动查询、对账三条路径使用同一个服务时钟，同一时刻对同一管道
 * 给出的健康结论与告警状态必须一致——恢复正常则告警当场恢复、不留残留、
 * 恢复记录可查、RECOVERY 通知恰好一条且不重复。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
class FixedClockAlertRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(FixedClockAlertRecoveryTest.class);
    private static final long T0 = ClockTestConfig.T0;
    private static final String P = "fixed-clock-rec";

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

    private void send(String runId, String status, long eventTime) throws Exception {
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId
                + "\",\"pipelineName\":\"" + P + "\",\"status\":\"" + status
                + "\",\"eventTime\":" + eventTime + "}";
        log.info("[INPUT] clock={} {}", ((MutableClock) clock).nowMillis(), json);
        var r = ingestion.ingest(mapper.readTree(json));
        log.info("[RESULT] accepted={} dup={} msg={}", r.isAccepted(), r.isDuplicate(), r.getMessage());
    }

    private List<Alert> activeOf(String pipeline) {
        return alerts.list(false).stream().filter(a -> pipeline.equals(a.getPipelineName())).toList();
    }

    @Test
    void alertRecoversUnderFixedClockAndVerdictsAgree() throws Exception {
        ((MutableClock) clock).set(T0);
        long threshold = 3_600_000L;
        log.info("[INPUT] fixed clock T0={} freshnessThreshold={}ms jitterGrace=300000ms", T0, threshold);

        // 1) 上一次成功停在 T0-4900s（已超阈值+宽限）：上报联动应判 STALE 并触发告警。
        send("fc-1", "RUNNING", T0 - 5_000_000L);
        send("fc-1", "SUCCESS", T0 - 4_900_000L);

        HealthAssessment viaQuery = health.assess(P, T0);
        List<Alert> active = activeOf(P);
        log.info("[CHECK] linked-reconcile verdict={} activeAlerts={}",
                viaQuery.getStatus(), active.stream().map(a -> a.getType() + "@" + a.getLastTriggeredAt()).toList());
        assertEquals(FreshnessStatus.STALE, viaQuery.getStatus());
        assertEquals(1, active.size(), "STALE verdict must come with exactly one active alert");
        assertEquals(AlertType.STALE, active.get(0).getType());
        assertEquals(T0, active.get(0).getLastTriggeredAt(),
                "alert must be stamped with the service clock, not the wall clock");

        // 2) 时钟不动，主动查询与对账必须给出与上报联动相同的结论。
        PipelineReconciliation recon = reconciliation.reconcile(P);
        log.info("[CHECK] reconcile verdict={} consistent={} basis={}",
                recon.getHealth().getStatus(), recon.isConsistent(), recon.getConsistencyBasis());
        assertEquals(FreshnessStatus.STALE, recon.getHealth().getStatus());
        assertEquals(T0, recon.getEvaluatedAt());
        assertTrue(recon.isConsistent());
        assertEquals(1, recon.getActiveAlerts().size());

        // 3) 补跑成功（仍固定在 T0）：上报联动当场判 HEALTHY，告警当场恢复。
        send("fc-2", "SUCCESS", T0 - 60_000L);
        HealthAssessment afterRecovery = health.assess(P, T0);
        List<Alert> activeAfter = activeOf(P);
        log.info("[CHECK] after-success verdict={} activeAlerts={}",
                afterRecovery.getStatus(), activeAfter.size());
        assertEquals(FreshnessStatus.HEALTHY, afterRecovery.getStatus(),
                "query path must agree with the ingest-linked verdict at the same instant");
        assertEquals(0, activeAfter.size(), "recovered pipeline must not keep any active alert");

        // 4) 恢复留痕：历史可查、恰好一条 RECOVERY 通知。
        List<Alert> history = alerts.list(true).stream()
                .filter(a -> P.equals(a.getPipelineName()) && a.getState() == AlertState.RECOVERED)
                .toList();
        long recoveryNotes = sink.notifications().stream()
                .filter(n -> n.pipeline().equals(P) && n.kind().equals("RECOVERY")).count();
        log.info("[CHECK] recoveredHistory={} recoveryNotifications={}", history.size(), recoveryNotes);
        assertEquals(1, history.size(), "recovery must be traceable in history");
        assertEquals(T0, history.get(0).getRecoveredAt());
        assertEquals(1, recoveryNotes, "exactly one RECOVERY notification");

        // 5) 对账同口径：HEALTHY 且无活跃告警，自洽校验通过。
        PipelineReconciliation ok = reconciliation.reconcile(P);
        log.info("[CHECK] recovered-reconcile verdict={} activeAlerts={} consistent={}",
                ok.getHealth().getStatus(), ok.getActiveAlerts().size(), ok.isConsistent());
        assertEquals(FreshnessStatus.HEALTHY, ok.getHealth().getStatus());
        assertEquals(0, ok.getActiveAlerts().size());
        assertTrue(ok.isConsistent());

        // 6) 重复恢复是空操作：无残留、不重复通知。
        assertNull(alerts.recover(P, AlertType.STALE, T0));
        long recoveryNotes2 = sink.notifications().stream()
                .filter(n -> n.pipeline().equals(P) && n.kind().equals("RECOVERY")).count();
        assertEquals(1, recoveryNotes2, "idempotent recovery must not re-notify");
        log.info("[ASSERT] fixed-clock recovery consistent, no residue, no duplicate notification");
    }
}

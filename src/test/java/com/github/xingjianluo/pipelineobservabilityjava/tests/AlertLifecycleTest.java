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

import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertSeverity;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertState;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.service.AlertService;
import com.github.xingjianluo.pipelineobservabilityjava.service.InMemoryNotificationSink;
import com.github.xingjianluo.pipelineobservabilityjava.service.MaintenanceService;

/** 告警触发/合并/升级/恢复/维护抑制：并发安全合并、恢复无残留且不重复通知。 */
@SpringBootTest
@Import(ClockTestConfig.class)
class AlertLifecycleTest {

    private static final Logger log = LoggerFactory.getLogger(AlertLifecycleTest.class);
    private static final long T0 = ClockTestConfig.T0;

    @Autowired
    private AlertService alerts;
    @Autowired
    private MaintenanceService maintenance;
    @Autowired
    private InMemoryNotificationSink sink;

    private HealthAssessment unhealthy(String pipeline) {
        HealthAssessment a = new HealthAssessment();
        a.setPipelineName(pipeline);
        a.setStatus(com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus.STALE);
        a.setEvaluatedAt(T0);
        a.getReasons().add("test stale");
        return a;
    }

    private HealthAssessment healthy(String pipeline, long at) {
        HealthAssessment a = new HealthAssessment();
        a.setPipelineName(pipeline);
        a.setStatus(com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus.HEALTHY);
        a.setEvaluatedAt(at);
        a.getReasons().add("test healthy");
        return a;
    }

    @Test
    void repeatedAlertsMergeCountAndNotifyOnce() {
        String p = "al-merge";
        for (int i = 0; i < 5; i++) {
            alerts.reconcile(unhealthy(p), T0 + i);
        }
        Alert a = alerts.list(false).stream()
                .filter(x -> x.getPipelineName().equals(p)).findFirst().orElseThrow();
        log.info("[MERGED] count={} state={} severity={}", a.getCount(), a.getState(), a.getSeverity());
        assertEquals(5, a.getCount());
        long firings = sink.notifications().stream()
                .filter(n -> n.pipeline().equals(p) && n.kind().equals("FIRING")).count();
        assertEquals(1, firings, "only one FIRING notification despite 5 reports");
    }

    @Test
    void escalationAfterThresholdAndRecoveryCleansUp() {
        String p = "al-escalate";
        alerts.reconcile(unhealthy(p), T0);
        // 持续超过升级时长(1800s)后的下一次观测触发升级
        alerts.reconcile(unhealthy(p), T0 + 1_800_000L);
        Alert a = alerts.list(false).stream()
                .filter(x -> x.getPipelineName().equals(p)).findFirst().orElseThrow();
        log.info("[ESCALATED] severity={} escalatedCount={}", a.getSeverity(), a.getEscalatedCount());
        assertEquals(AlertSeverity.CRITICAL, a.getSeverity());
        assertEquals(1, a.getEscalatedCount());

        // 恢复：活跃表无残留、历史可查、恰好一条 RECOVERY
        alerts.reconcile(healthy(p, T0 + 1_800_001L), T0 + 1_800_001L);
        Alert recovered = alerts.list(true).stream().filter(x -> x.getPipelineName().equals(p)
                && x.getState() == AlertState.RECOVERED).findFirst().orElseThrow();
        long active = alerts.list(false).stream().filter(x -> x.getPipelineName().equals(p)).count();
        long recoveries = sink.notifications().stream()
                .filter(n -> n.pipeline().equals(p) && n.kind().equals("RECOVERY")).count();
        log.info("[RECOVERED] activeLeft={} recoveryNotes={} historyPresent={}",
                active, recoveries, recovered != null);
        assertEquals(0, active, "no residual active alert after recovery");
        assertEquals(1, recoveries, "exactly one recovery notification");
        assertTrue(recovered != null, "recovered alert still traceable in history");

        // 重复恢复为空操作，不再发通知
        assertNull(alerts.recover(p, AlertType.STALE, T0 + 1_900_000L));
        long recoveries2 = sink.notifications().stream()
                .filter(n -> n.pipeline().equals(p) && n.kind().equals("RECOVERY")).count();
        assertEquals(1, recoveries2);
    }

    @Test
    void maintenanceSuppressesNotificationsButKeepsTrace() {
        String p = "al-maint";
        // 维护窗口覆盖 T0..T0+1000
        maintenance.add(p, T0, T0 + 1000, "deploy");
        alerts.reconcile(unhealthy(p), T0 + 50);
        alerts.reconcile(unhealthy(p), T0 + 100);
        Alert suppressed = alerts.list(false).stream()
                .filter(x -> x.getPipelineName().equals(p)).findFirst().orElseThrow();
        log.info("[SUPPRESSED] state={} count={} suppressedCount={}",
                suppressed.getState(), suppressed.getCount(), suppressed.getSuppressedCount());
        assertEquals(AlertState.SUPPRESSED, suppressed.getState());
        assertEquals(2, suppressed.getCount());
        assertEquals(2, suppressed.getSuppressedCount());
        long firings = sink.notifications().stream()
                .filter(n -> n.pipeline().equals(p)).count();
        assertEquals(0, firings, "no notification while in maintenance window");

        // 窗口结束后仍不健康：补发一次 FIRING（同一事件延续，不新开告警）
        alerts.reconcile(unhealthy(p), T0 + 1001);
        Alert firing = alerts.list(false).stream()
                .filter(x -> x.getPipelineName().equals(p)).findFirst().orElseThrow();
        assertEquals(AlertState.FIRING, firing.getState());
        assertEquals(3, firing.getCount());
        long firingsAfter = sink.notifications().stream()
                .filter(n -> n.pipeline().equals(p) && n.kind().equals("FIRING")).count();
        assertEquals(1, firingsAfter, "single re-fire after maintenance ends");
        log.info("[POST-MAINT] state={} count={} firingNotes={}",
                firing.getState(), firing.getCount(), firingsAfter);
    }
}

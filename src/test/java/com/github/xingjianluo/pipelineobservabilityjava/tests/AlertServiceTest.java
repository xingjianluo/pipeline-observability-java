package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertLevel;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertRecord;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertState;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthSnapshot;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthStatus;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.NotificationKind;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.AlertService;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.MaintenanceService;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.AlertStore;
import com.github.xingjianluo.pipelineobservabilityjava.observability.util.MutableClock;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 告警生命周期：触发去重合并（不重复通知）、持续超时升级一次、恢复一次不残留、
 * 维护窗口抑制但留痕、窗口结束后补发/恢复。
 */
class AlertServiceTest {

    private static final Logger log = LoggerFactory.getLogger(AlertServiceTest.class);

    private HealthSnapshot firing(String pipeline, long at, AlertType type, String reasonPrefix) {
        return new HealthSnapshot(pipeline, at, HealthStatus.STALE,
                List.of(reasonPrefix + ": test basis"), null, null, null, Set.of(type));
    }

    private HealthSnapshot healthy(String pipeline, long at) {
        return new HealthSnapshot(pipeline, at, HealthStatus.HEALTHY,
                List.of("HEALTHY: 判定正常"), null, null, null, Set.of());
    }

    @Test
    void triggerMergeEscalateAndRecover() {
        MutableClock clock = new MutableClock();
        clock.setNow(10_000L);
        AlertStore store = new AlertStore();
        MaintenanceService maintenance = new MaintenanceService(clock);
        AlertService alerts = new AlertService(store, maintenance, clock);

        // 连续三轮触发：第一轮换 1 条 TRIGGERED，后两轮仅合并计数
        alerts.reconcile(firing("p1", 10_000, AlertType.FRESHNESS, "STALE"), 5_000);
        alerts.reconcile(firing("p1", 11_000, AlertType.FRESHNESS, "STALE"), 5_000);
        alerts.reconcile(firing("p1", 12_000, AlertType.FRESHNESS, "STALE"), 5_000);

        AlertRecord active = store.get(AlertStore.key("p1", AlertType.FRESHNESS)).orElseThrow();
        long triggeredCount = store.notifications().stream()
                .filter(n -> n.kind() == NotificationKind.TRIGGERED).count();
        log.info("[去重合并] 三轮触发 count={} TRIGGERED通知数={} state={}",
                active.count(), triggeredCount, active.state());
        assertEquals(3, active.count());
        assertEquals(1, triggeredCount, "重复告警不得重复通知");
        assertEquals(AlertState.ACTIVE, active.state());

        // 持续时间严格大于 5000ms 才升级：15000(边界) 不升级，15001 升级一次
        alerts.reconcile(firing("p1", 15_000, AlertType.FRESHNESS, "STALE"), 5_000);
        assertEquals(AlertLevel.WARNING,
                store.get(AlertStore.key("p1", AlertType.FRESHNESS)).orElseThrow().level());
        alerts.reconcile(firing("p1", 15_001, AlertType.FRESHNESS, "STALE"), 5_000);
        AlertRecord escalated = store.get(AlertStore.key("p1", AlertType.FRESHNESS)).orElseThrow();
        log.info("[升级] at=15000边界未升级; at=15001 state={} level={}", escalated.state(), escalated.level());
        assertEquals(AlertState.ESCALATED, escalated.state());
        assertEquals(AlertLevel.CRITICAL, escalated.level());
        long escalations = store.notifications().stream()
                .filter(n -> n.kind() == NotificationKind.ESCALATED).count();
        assertEquals(1, escalations, "每个告警仅升级一次");

        // 恢复：恰好一条 RECOVERED，且列表中不再有活跃告警
        alerts.reconcile(healthy("p1", 16_000), 5_000);
        AlertRecord recovered = store.get(AlertStore.key("p1", AlertType.FRESHNESS)).orElseThrow();
        long recoveries = store.notifications().stream()
                .filter(n -> n.kind() == NotificationKind.RECOVERED).count();
        log.info("[恢复] state={} RECOVERED通知数={}", recovered.state(), recoveries);
        assertEquals(AlertState.RECOVERED, recovered.state());
        assertEquals(1, recoveries);
        assertFalse(alerts.listAlerts(false).stream()
                .anyMatch(a -> a.pipelineId().equals("p1")), "恢复后不得残留活跃告警");

        // 再触发一轮：视为新告警，重新 TRIGGERED 一次（计数从 1 开始）
        alerts.reconcile(firing("p1", 20_000, AlertType.FRESHNESS, "STALE"), 5_000);
        AlertRecord refired = store.get(AlertStore.key("p1", AlertType.FRESHNESS)).orElseThrow();
        long triggeredAgain = store.notifications().stream()
                .filter(n -> n.kind() == NotificationKind.TRIGGERED).count();
        log.info("[恢复后再触发] count={} TRIGGERED累计={} state={}",
                refired.count(), triggeredAgain, refired.state());
        assertEquals(1, refired.count());
        assertEquals(2, triggeredAgain);
    }

    @Test
    void maintenanceSuppressesButIsTraceable() {
        MutableClock clock = new MutableClock();
        clock.setNow(0L);
        AlertStore store = new AlertStore();
        MaintenanceService maintenance = new MaintenanceService(clock);
        AlertService alerts = new AlertService(store, maintenance, clock);

        maintenance.openWindow("p1", 0L, 10_000L, "deploy");
        alerts.reconcile(firing("p1", 1_000, AlertType.FRESHNESS, "STALE"), 5_000);
        alerts.reconcile(firing("p1", 2_000, AlertType.FRESHNESS, "STALE"), 5_000);

        AlertRecord suppressed = store.get(AlertStore.key("p1", AlertType.FRESHNESS)).orElseThrow();
        long notificationsDuringWindow = store.notifications().size();
        log.info("[维护抑制] state={} suppressed={} count={} 窗口内通知数={}",
                suppressed.state(), suppressed.suppressed(), suppressed.count(), notificationsDuringWindow);
        assertEquals(AlertState.SUPPRESSED, suppressed.state());
        assertTrue(suppressed.suppressed());
        assertEquals(2, suppressed.count(), "抑制期重复仍要合并计数、可追溯");
        assertEquals(0, notificationsDuringWindow, "维护窗口内不得发送通知");

        // 窗口结束问题仍在：恢复 ACTIVE 并补发一次 TRIGGERED
        alerts.reconcile(firing("p1", 10_001, AlertType.FRESHNESS, "STALE"), 5_000);
        AlertRecord active = store.get(AlertStore.key("p1", AlertType.FRESHNESS)).orElseThrow();
        long triggered = store.notifications().stream()
                .filter(n -> n.kind() == NotificationKind.TRIGGERED).count();
        log.info("[窗口结束仍未恢复] state={} TRIGGERED={} count={}",
                active.state(), triggered, active.count());
        assertEquals(AlertState.ACTIVE, active.state());
        assertEquals(1, triggered);

        // 窗口结束且问题消失：恢复通知一次
        alerts.reconcile(healthy("p1", 11_000), 5_000);
        AlertRecord recovered = store.get(AlertStore.key("p1", AlertType.FRESHNESS)).orElseThrow();
        log.info("[恢复] state={} RECOVERED={}", recovered.state(),
                store.notifications().stream().filter(n -> n.kind() == NotificationKind.RECOVERED).count());
        assertEquals(AlertState.RECOVERED, recovered.state());
    }
}

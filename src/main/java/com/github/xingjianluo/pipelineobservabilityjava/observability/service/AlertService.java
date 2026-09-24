package com.github.xingjianluo.pipelineobservabilityjava.observability.service;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertLevel;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertNotification;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertRecord;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertState;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthSnapshot;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.NotificationKind;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.AlertStore;
import com.github.xingjianluo.pipelineobservabilityjava.observability.util.MutableClock;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 告警生命周期协调（每个管道每轮评估调用一次 {@link #reconcile}）。
 *
 * <ul>
 *   <li><b>触发/去重</b>：同 key 告警已存在（ACTIVE/ESCALATED/SUPPRESSED）则只合并计数，
 *       不重复发送 TRIGGERED；新类型首次触发才通知；</li>
 *   <li><b>升级</b>：ACTIVE 且持续时间 &gt; escalationAfterMs 升级 CRITICAL，每个 key 仅升级一次
 *       （严格大于，边界不升级）；</li>
 *   <li><b>抑制</b>：维护窗口内首次触发进入 SUPPRESSED，不发通知但记录留痕；抑制期重复仅加计数；
 *       窗口结束后仍在触发则下一轮重新发 TRIGGERED；</li>
 *   <li><b>恢复</b>：本轮不再触发的活跃告警（含抑制中的）发送一次 RECOVERED 后置 RECOVERED，
 *       不残留 ACTIVE；后续若再次触发视为新一轮，重新 TRIGGERED。</li>
 * </ul>
 */
@Service
public class AlertService {

    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    private static final Set<AlertType> ALL_TYPES = EnumSet.allOf(AlertType.class);

    private final AlertStore alertStore;
    private final MaintenanceService maintenanceService;
    private final MutableClock clock;

    public AlertService(AlertStore alertStore, MaintenanceService maintenanceService, MutableClock clock) {
        this.alertStore = alertStore;
        this.maintenanceService = maintenanceService;
        this.clock = clock;
    }

    public void reconcile(HealthSnapshot snapshot, long escalationAfterMs) {
        String pipelineId = snapshot.pipelineId();
        long now = snapshot.evaluatedAt();
        boolean suppressedNow = maintenanceService.isSuppressed(pipelineId, now);

        alertStore.reconcile(() -> {
            // 1) 本轮正在触发的类型：触发去重合并 / 升级 / 从恢复态重新打开
            for (AlertType type : snapshot.firingAlertTypes()) {
                reconcileFiring(pipelineId, type, snapshot, suppressedNow, escalationAfterMs, now);
            }
            // 2) 本轮未触发但仍活跃的类型：恢复（不残留）
            for (AlertType type : ALL_TYPES) {
                if (!snapshot.firingAlertTypes().contains(type)) {
                    recoverIfActive(pipelineId, type, now);
                }
            }
        });
    }

    private void reconcileFiring(String pipelineId, AlertType type, HealthSnapshot snapshot,
                                 boolean suppressedNow, long escalationAfterMs, long now) {
        String key = AlertStore.key(pipelineId, type);
        AlertRecord prev = alertStore.get(key).orElse(null);
        String reason = reasonFor(snapshot, type);

        if (prev == null || prev.state() == AlertState.RECOVERED) {
            // 首次触发，或恢复后再次触发（新一轮）
            if (suppressedNow) {
                AlertRecord record = new AlertRecord(key, pipelineId, type, AlertState.SUPPRESSED,
                        AlertLevel.WARNING, 1, now, now, null, null, true, reason, 1);
                put(record);
                log.info("AlertService.suppressed | key={} at={} reason={}（维护窗口内抑制，不发通知，仍留痕）",
                        key, now, reason);
            } else {
                AlertRecord record = new AlertRecord(key, pipelineId, type, AlertState.ACTIVE,
                        AlertLevel.WARNING, 1, now, now, null, null, false, reason, 1);
                put(record);
                notify(key, pipelineId, type, AlertLevel.WARNING, NotificationKind.TRIGGERED, 1, reason, now);
            }
            return;
        }

        // 已存在且未恢复：重复告警合并计数（并发/连续评估都只加计数，绝不重复通知）
        int newCount = prev.count() + 1;
        AlertState state = prev.state();
        AlertLevel level = prev.level();
        Long escalatedAt = prev.escalatedAt();

        if (state == AlertState.SUPPRESSED && !suppressedNow) {
            // 维护窗口结束、问题仍在：恢复为 ACTIVE 并补发一次触发通知
            state = AlertState.ACTIVE;
            AlertRecord record = new AlertRecord(key, pipelineId, type, state, level, newCount,
                    prev.firstTriggeredAt(), now, escalatedAt, null, false, reason, prev.version() + 1);
            put(record);
            notify(key, pipelineId, type, level, NotificationKind.TRIGGERED, newCount,
                    "维护窗口结束，问题仍在，恢复通知：" + reason, now);
            return;
        }
        if (state == AlertState.ACTIVE && suppressedNow) {
            state = AlertState.SUPPRESSED;
        }

        // 升级：仅 ACTIVE（非抑制）、WARNING 且严格超过升级时长，升级一次
        if (state == AlertState.ACTIVE && level == AlertLevel.WARNING
                && escalationAfterMs > 0
                && now - prev.firstTriggeredAt() > escalationAfterMs) {
            state = AlertState.ESCALATED;
            level = AlertLevel.CRITICAL;
            escalatedAt = now;
            AlertRecord record = new AlertRecord(key, pipelineId, type, state, level, newCount,
                    prev.firstTriggeredAt(), now, escalatedAt, null, false, reason, prev.version() + 1);
            put(record);
            notify(key, pipelineId, type, AlertLevel.CRITICAL, NotificationKind.ESCALATED,
                    newCount, "持续 " + (now - prev.firstTriggeredAt()) + "ms 超过升级阈值 "
                            + escalationAfterMs + "ms：" + reason, now);
            return;
        }

        AlertRecord merged = new AlertRecord(key, pipelineId, type, state, level, newCount,
                prev.firstTriggeredAt(), now, escalatedAt, null, state == AlertState.SUPPRESSED,
                reason, prev.version() + 1);
        put(merged);
        log.info("AlertService.merged | key={} state={} count={}（重复告警合并，不重复通知）",
                key, state, newCount);
    }

    private void recoverIfActive(String pipelineId, AlertType type, long now) {
        String key = AlertStore.key(pipelineId, type);
        AlertRecord prev = alertStore.get(key).orElse(null);
        if (prev == null || prev.state() == AlertState.RECOVERED) {
            return;
        }
        AlertRecord recovered = new AlertRecord(key, pipelineId, type, AlertState.RECOVERED,
                prev.level(), prev.count(), prev.firstTriggeredAt(), prev.lastTriggeredAt(),
                prev.escalatedAt(), now, false, prev.lastReason(), prev.version() + 1);
        put(recovered);
        // 无论触发时是否处于抑制，恢复都给一次且仅一次通知，保证“恢复后不重复通知、不残留”
        notify(key, pipelineId, type, prev.level(), NotificationKind.RECOVERED, prev.count(),
                prev.state() == AlertState.SUPPRESSED
                        ? "维护窗口结束且问题已自行恢复" : "判定恢复：" + prev.lastReason(), now);
        log.info("AlertService.recovered | key={} at={} count={}", key, now, prev.count());
    }

    private void put(AlertRecord record) {
        alertStore.upsert(record.pipelineId(), record.alertType(), prev -> record);
    }

    private void notify(String key, String pipelineId, AlertType type, AlertLevel level,
                        NotificationKind kind, int count, String reason, long now) {
        alertStore.appendNotification(new AlertNotification(now, key, pipelineId, type, level,
                kind, count, reason));
    }

    private String reasonFor(HealthSnapshot snapshot, AlertType type) {
        return snapshot.reasons().stream()
                .filter(r -> r.startsWith(type.name()) || r.startsWith(prefixOf(type)))
                .findFirst()
                .orElse(snapshot.status().name());
    }

    private String prefixOf(AlertType type) {
        return switch (type) {
            case FRESHNESS -> "NEVER_SUCCEEDED";
            case SCHEDULE_LAG -> "SCHEDULE_LAGGING";
            case SLA_BREACH -> "SLA_BREACHED";
        };
    }

    public List<AlertRecord> listAlerts(boolean includeRecovered) {
        return alertStore.all().stream()
                .filter(a -> includeRecovered || a.state() != AlertState.RECOVERED)
                .toList();
    }

    public List<AlertNotification> notifications() {
        return alertStore.notifications();
    }
}

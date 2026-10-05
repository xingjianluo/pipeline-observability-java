package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.xingjianluo.pipelineobservabilityjava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertSeverity;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertState;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;

@Service
public class DefaultAlertService implements AlertService {

    private static final Logger log = LoggerFactory.getLogger(DefaultAlertService.class);

    private final ObservabilityProperties properties;
    private final MaintenanceService maintenanceService;
    private final NotificationSink sink;

    /** 活跃告警（FIRING/SUPPRESSED），同一 (pipeline,type) 只有一条。 */
    private final ConcurrentMap<String, Alert> active = new ConcurrentHashMap<>();
    /** 已恢复告警的不可变历史快照，恢复后仍可追溯但不残留在活跃列表。 */
    private final CopyOnWriteArrayList<Alert> recoveredHistory = new CopyOnWriteArrayList<>();

    public DefaultAlertService(ObservabilityProperties properties,
                               MaintenanceService maintenanceService,
                               NotificationSink sink) {
        this.properties = properties;
        this.maintenanceService = maintenanceService;
        this.sink = sink;
    }

    private static String key(String pipeline, AlertType type) {
        return pipeline + "|" + type.name();
    }

    @Override
    public Alert reconcile(HealthAssessment assessment, long atMillis) {
        AlertType type = map(assessment.getStatus());
        // 连续错过多轮调度：结论随轮次加重，1 轮 WARNING，>=2 轮直接 CRITICAL。
        AlertSeverity desired = assessment.getStatus() == FreshnessStatus.SCHEDULE_LAGGING
                && assessment.getMissedRounds() >= 2
                ? AlertSeverity.CRITICAL : AlertSeverity.WARNING;
        log.info("reconcile pipeline={} verdict={} alertType={} missed={} severity={} at={}",
                assessment.getPipelineName(), assessment.getStatus(), type,
                assessment.getMissedRounds(), desired, atMillis);
        if (type == null) {
            // 健康/抖动/从未成功：确保该管道所有健康类与失败类活跃告警被幂等恢复。
            if (assessment.getStatus() == FreshnessStatus.HEALTHY) {
                for (AlertType t : List.of(AlertType.STALE, AlertType.SCHEDULE_LAGGING,
                        AlertType.SLA_BREACHED, AlertType.RUN_FAILED)) {
                    recover(assessment.getPipelineName(), t, atMillis);
                }
            }
            return null;
        }
        return merge(assessment.getPipelineName(), type, atMillis,
                assessment.getStatus().name() + ": " + String.join("; ", assessment.getReasons()),
                desired);
    }

    private static AlertType map(FreshnessStatus s) {
        return switch (s) {
            case STALE -> AlertType.STALE;
            case SCHEDULE_LAGGING -> AlertType.SCHEDULE_LAGGING;
            case SLA_BREACHED -> AlertType.SLA_BREACHED;
            default -> null;
        };
    }

    @Override
    public Alert reportFailure(String pipelineName, String runId, long atMillis, String detail) {
        return merge(pipelineName, AlertType.RUN_FAILED, atMillis,
                "runId=" + runId + " " + detail, AlertSeverity.WARNING);
    }

    @Override
    public long mergedCountForPipeline(String pipelineName) {
        return active.values().stream()
                .filter(a -> a.getPipelineName().equals(pipelineName))
                .mapToLong(Alert::getCount).sum();
    }

    @Override
    public long suppressedCountForPipeline(String pipelineName) {
        return active.values().stream()
                .filter(a -> a.getPipelineName().equals(pipelineName))
                .mapToLong(Alert::getSuppressedCount).sum();
    }

    @Override
    public long notifiedCountForPipeline(String pipelineName) {
        return active.values().stream()
                .filter(a -> a.getPipelineName().equals(pipelineName))
                .mapToLong(Alert::getNotifiedCount).sum();
    }

    /**
     * 原子合并：ConcurrentMap.compute 对同一 key 串行，并发上报计数绝不丢失。
     * 迟到（atMillis 早于已处理时间）的观测直接忽略，不回退结论。
     *
     * @param desiredSeverity 本次观测期望的级别（如连续错过 2 轮为 CRITICAL）；
     *                        只升不降，避免抖动导致级别回退。
     */
    private Alert merge(String pipeline, AlertType type, long atMillis, String detail,
                        AlertSeverity desiredSeverity) {
        boolean suppressedNow = maintenanceService.isSuppressed(pipeline, atMillis);
        Alert result = active.compute(key(pipeline, type), (k, cur) -> {
            if (cur == null) {
                Alert a = newAlert(pipeline, type, atMillis, detail);
                a.setCount(1);
                if (desiredSeverity == AlertSeverity.CRITICAL) {
                    a.setSeverity(AlertSeverity.CRITICAL);
                }
                if (suppressedNow) {
                    a.setState(AlertState.SUPPRESSED);
                    a.setSuppressedCount(1);
                } else {
                    a.setState(AlertState.FIRING);
                    a.setNotifiedCount(a.getNotifiedCount() + 1);
                    sink.notifyFiring(copyOf(a));
                }
                log.info("alert triggered pipeline={} type={} suppressed={} severity={} at={}",
                        pipeline, type, suppressedNow, a.getSeverity(), atMillis);
                return a;
            }
            // 迟到观测保护：不允许旧观测改动新状态或重复通知。
            if (atMillis < cur.getLastTriggeredAt()) {
                log.info("late alert observation ignored pipeline={} type={} at={} < {}",
                        pipeline, type, atMillis, cur.getLastTriggeredAt());
                return cur;
            }
            cur.setCount(cur.getCount() + 1);
            cur.setLastTriggeredAt(atMillis);
            cur.setDetail(detail);
            // 观测结论加重（错过更多轮次）时升级级别，只升不降。
            if (desiredSeverity == AlertSeverity.CRITICAL
                    && cur.getSeverity() != AlertSeverity.CRITICAL) {
                cur.setSeverity(AlertSeverity.CRITICAL);
                cur.setEscalatedCount(cur.getEscalatedCount() + 1);
                log.info("alert upgraded by severity pipeline={} type={} -> CRITICAL", pipeline, type);
            }

            if (suppressedNow) {
                // 维护窗口内：合并计数但抑制一切通知；从 FIRING 转入 SUPPRESSED。
                cur.setSuppressedCount(cur.getSuppressedCount() + 1);
                if (cur.getState() != AlertState.SUPPRESSED) {
                    cur.setState(AlertState.SUPPRESSED);
                    log.info("alert suppressed by maintenance pipeline={} type={}", pipeline, type);
                }
                maybeEscalate(cur, atMillis, false);
                return cur;
            }

            if (cur.getState() == AlertState.SUPPRESSED) {
                // 维护窗口刚结束：恢复对外通知，但这是同一起事件的延续，只补发一次 FIRING。
                cur.setState(AlertState.FIRING);
                cur.setNotifiedCount(cur.getNotifiedCount() + 1);
                sink.notifyFiring(copyOf(cur));
                log.info("alert re-fired after maintenance pipeline={} type={} severity={}",
                        pipeline, type, cur.getSeverity());
            }
            maybeEscalate(cur, atMillis, true);
            return cur;
        });
        return copyOf(result);
    }

    /** 达到升级时长则 WARNING -> CRITICAL；抑制期间只升级级别、不发升级通知。 */
    private void maybeEscalate(Alert a, long atMillis, boolean allowNotify) {
        if (a.getSeverity() == AlertSeverity.WARNING
                && atMillis - a.getFirstTriggeredAt() >= properties.getEscalationAfterMs()) {
            a.setSeverity(AlertSeverity.CRITICAL);
            a.setEscalatedCount(a.getEscalatedCount() + 1);
            if (allowNotify) {
                a.setNotifiedCount(a.getNotifiedCount() + 1);
                sink.notifyEscalation(copyOf(a));
            }
            log.info("alert escalated pipeline={} type={} at={} notified={}",
                    a.getPipelineName(), a.getType(), atMillis, allowNotify);
        }
    }

    private Alert newAlert(String pipeline, AlertType type, long atMillis, String detail) {
        Alert a = new Alert();
        a.setAlertId(pipeline + ":" + type.name());
        a.setPipelineName(pipeline);
        a.setType(type);
        a.setSeverity(AlertSeverity.WARNING);
        a.setFirstTriggeredAt(atMillis);
        a.setLastTriggeredAt(atMillis);
        a.setDetail(detail);
        return a;
    }

    @Override
    public Alert recover(String pipelineName, AlertType type, long atMillis) {
        Alert[] removed = new Alert[1];
        active.computeIfPresent(key(pipelineName, type), (k, cur) -> {
            if (atMillis < cur.getLastTriggeredAt()) {
                // 旧时刻的恢复指令不得抹掉新告警。
                log.info("late recovery ignored pipeline={} type={} at={} < {}",
                        pipelineName, type, atMillis, cur.getLastTriggeredAt());
                return cur;
            }
            cur.setState(AlertState.RECOVERED);
            cur.setRecoveredAt(atMillis);
            removed[0] = copyOf(cur);
            // 仅当此前真正对外通知过时才发恢复通知；纯抑制告警静默关闭，避免无来源的通知。
            if (cur.getNotifiedCount() > 0) {
                sink.notifyRecovery(removed[0]);
            }
            log.info("alert recovered pipeline={} type={} at={} hadNotified={}",
                    pipelineName, type, atMillis, cur.getNotifiedCount() > 0);
            return null; // 从活跃表移除，保证“恢复后不残留”
        });
        if (removed[0] != null) {
            recoveredHistory.add(removed[0]);
            return removed[0];
        }
        return null; // 幂等：无活跃告警时恢复是空操作，绝不重复通知
    }

    @Override
    public List<Alert> list(boolean includeRecovered) {
        List<Alert> all = new ArrayList<>();
        active.values().forEach(a -> all.add(copyOf(a)));
        if (includeRecovered) {
            all.addAll(recoveredHistory);
        }
        return List.copyOf(all);
    }

    private static Alert copyOf(Alert a) {
        Alert c = new Alert();
        c.setAlertId(a.getAlertId());
        c.setPipelineName(a.getPipelineName());
        c.setType(a.getType());
        c.setState(a.getState());
        c.setSeverity(a.getSeverity());
        c.setFirstTriggeredAt(a.getFirstTriggeredAt());
        c.setLastTriggeredAt(a.getLastTriggeredAt());
        c.setCount(a.getCount());
        c.setEscalatedCount(a.getEscalatedCount());
        c.setRecoveredAt(a.getRecoveredAt());
        c.setDetail(a.getDetail());
        c.setSuppressedCount(a.getSuppressedCount());
        c.setNotifiedCount(a.getNotifiedCount());
        c.setVersion(a.getVersion());
        return c;
    }
}

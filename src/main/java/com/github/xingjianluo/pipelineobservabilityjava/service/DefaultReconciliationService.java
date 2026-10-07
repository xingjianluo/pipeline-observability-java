package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineReconciliation;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineRunState;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/**
 * 对账实现：整个“评估 + 告警对账 + 计数快照”在与上报互斥的一致性守卫内、
 * 同一时刻一次取齐，保证同一响应里的健康结论、告警状态与各类计数同口径。
 * 返回前做自洽校验并把依据写入 consistencyNotes；真出现矛盾时重取一次，
 * 仍矛盾则抛异常，绝不把矛盾结果悄悄返回。
 */
@Service
public class DefaultReconciliationService implements ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(DefaultReconciliationService.class);

    private final RunStateStore store;
    private final HealthEvaluationService healthEvaluationService;
    private final AlertService alertService;
    private final RejectionLogService rejectionLog;
    private final SystemClock clock;
    private final ConsistencyGuard consistencyGuard;

    public DefaultReconciliationService(RunStateStore store,
                                        HealthEvaluationService healthEvaluationService,
                                        AlertService alertService,
                                        RejectionLogService rejectionLog,
                                        SystemClock clock,
                                        ConsistencyGuard consistencyGuard) {
        this.store = store;
        this.healthEvaluationService = healthEvaluationService;
        this.alertService = alertService;
        this.rejectionLog = rejectionLog;
        this.clock = clock;
        this.consistencyGuard = consistencyGuard;
    }

    @Override
    public PipelineReconciliation reconcile(String pipelineName) {
        // 与上报互斥的一致快照：守卫内不会出现并发上报穿插改动状态。
        PipelineReconciliation r = consistencyGuard.atomically(() -> snapshot(pipelineName));
        List<String> violations = validate(r);
        if (!violations.isEmpty()) {
            // 兜底：理论上守卫内不会矛盾；真出现时重取一次再校验，仍矛盾则拒绝返回。
            log.warn("reconciliation self-check failed pipeline={} violations={}, retrying once",
                    pipelineName, violations);
            PipelineReconciliation retry = consistencyGuard.atomically(() -> snapshot(pipelineName));
            List<String> retryViolations = validate(retry);
            if (!retryViolations.isEmpty()) {
                throw new ReconciliationInconsistentException(pipelineName, retryViolations);
            }
            retry.getConsistencyNotes().add("self-check passed on retry");
            log.info("reconciled pipeline={} at={} verdict={} consistent=true (after retry)",
                    pipelineName, retry.getEvaluatedAt(), retry.getHealth().getStatus());
            return retry;
        }
        log.info("reconciled pipeline={} at={} verdict={} activeAlerts={} received={} dup={} "
                        + "late={} rejected={} alertMerged={} consistent=true",
                pipelineName, r.getEvaluatedAt(), r.getHealth().getStatus(),
                r.getActiveAlerts().size(), r.getReceivedEvents(), r.getDuplicateEvents(),
                r.getLateEvents(), r.getRejectedEvents(), r.getAlertMergedCount());
        return r;
    }

    /** 在一致性守卫内取同一时刻的完整快照：先评估并联动告警，再按同一时刻快照计数。 */
    private PipelineReconciliation snapshot(String pipelineName) {
        long now = clock.nowMillis();
        // 1) 统一时刻评估并联动告警对账：结论不含的健康类告警当场幂等恢复。
        HealthAssessment assessment = healthEvaluationService.assessAndReconcile(pipelineName, now);

        // 2) 计数快照：接收/重复/迟到来自运行状态（并发下原子累加，不丢不重）。
        List<PipelineRunState> runs = store.findAll().stream()
                .filter(s -> pipelineName.equals(s.getPipelineName()))
                .toList();
        long received = runs.stream().mapToLong(PipelineRunState::getEventCount).sum();
        long duplicates = runs.stream().mapToLong(PipelineRunState::getDuplicateCount).sum();
        long late = runs.stream().mapToLong(PipelineRunState::getLateIgnoredCount).sum();

        // 3) 告警快照与评估同口径：活跃告警反映的正是上面这次评估的结论。
        List<Alert> active = alertService.list(false).stream()
                .filter(al -> pipelineName.equals(al.getPipelineName()))
                .toList();
        long merged = alertService.list(true).stream()
                .filter(al -> pipelineName.equals(al.getPipelineName()))
                .mapToLong(Alert::getCount).sum();

        PipelineReconciliation r = new PipelineReconciliation();
        r.setPipelineName(pipelineName);
        r.setEvaluatedAt(now);
        r.setHealth(assessment);
        r.setActiveAlerts(active);
        r.setReceivedEvents(received);
        r.setDuplicateEvents(duplicates);
        r.setLateEvents(late);
        r.setAppliedEvents(received - duplicates - late);
        r.setRejectedEvents(rejectionLog.totalCount(pipelineName));
        r.setRejectionsByCode(rejectionLog.countByCode(pipelineName));
        r.setAlertMergedCount(merged);
        r.setRunsTracked(runs.size());
        return r;
    }

    /**
     * 自洽校验：通过则写入判断依据（consistent=true），失败返回矛盾点列表。
     * 校验项：计数勾稽、被拒汇总勾稽、健康结论与活跃告警同口径。
     */
    private List<String> validate(PipelineReconciliation r) {
        List<String> violations = new ArrayList<>();
        List<String> notes = new ArrayList<>();
        FreshnessStatus status = r.getHealth().getStatus();

        if (r.getAppliedEvents() == r.getReceivedEvents() - r.getDuplicateEvents() - r.getLateEvents()) {
            notes.add("counters reconcile: applied=" + r.getAppliedEvents() + " = received "
                    + r.getReceivedEvents() + " - duplicates " + r.getDuplicateEvents()
                    + " - late " + r.getLateEvents());
        } else {
            violations.add("appliedEvents " + r.getAppliedEvents() + " != received "
                    + r.getReceivedEvents() + " - duplicates " + r.getDuplicateEvents()
                    + " - late " + r.getLateEvents());
        }

        long rejectedSum = r.getRejectionsByCode().values().stream().mapToLong(Long::longValue).sum();
        if (rejectedSum == r.getRejectedEvents()) {
            notes.add("rejections reconcile: total=" + r.getRejectedEvents()
                    + " equals sum of per-code counts " + r.getRejectionsByCode());
        } else {
            violations.add("rejectedEvents " + r.getRejectedEvents()
                    + " != sum of rejectionsByCode " + rejectedSum);
        }

        AlertType expected = switch (status) {
            case STALE -> AlertType.STALE;
            case SCHEDULE_LAGGING -> AlertType.SCHEDULE_LAGGING;
            case SLA_BREACHED -> AlertType.SLA_BREACHED;
            default -> null;
        };
        boolean expectedActive = expected != null && r.getActiveAlerts().stream()
                .anyMatch(a -> a.getType() == expected);
        boolean noHealthAlertActive = r.getActiveAlerts().stream()
                .noneMatch(a -> a.getType() == AlertType.STALE
                        || a.getType() == AlertType.SCHEDULE_LAGGING
                        || a.getType() == AlertType.SLA_BREACHED);
        if (expected != null && expectedActive) {
            notes.add("health/alert same viewpoint: verdict " + status
                    + " comes with active " + expected + " alert");
        } else if (expected == null && noHealthAlertActive) {
            notes.add("health/alert same viewpoint: verdict " + status
                    + " produces no health-driven alert and none is active");
        } else {
            violations.add("verdict " + status + " inconsistent with active alerts "
                    + r.getActiveAlerts().stream().map(a -> a.getType().name()).toList());
        }
        if (status == FreshnessStatus.HEALTHY && !r.getActiveAlerts().isEmpty()) {
            violations.add("HEALTHY verdict must not coexist with any active alert, got "
                    + r.getActiveAlerts().stream().map(a -> a.getType().name()).toList());
        } else if (status == FreshnessStatus.HEALTHY) {
            notes.add("HEALTHY verdict coexists with zero active alerts");
        }

        if (violations.isEmpty()) {
            r.setConsistent(true);
            r.setConsistencyNotes(notes);
        }
        return violations;
    }

    /** 对账自洽校验持续失败时抛出，绝不把矛盾结果悄悄返回。 */
    public static final class ReconciliationInconsistentException extends RuntimeException {
        private final transient List<String> violations;

        ReconciliationInconsistentException(String pipelineName, List<String> violations) {
            super("reconciliation for pipeline '" + pipelineName
                    + "' is not self-consistent: " + violations);
            this.violations = List.copyOf(violations);
        }

        public List<String> getViolations() {
            return violations;
        }
    }
}

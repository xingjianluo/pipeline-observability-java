package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

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
 * 对账实现：整个“评估 + 告警对账 + 计数快照”在与上报共用的互斥锁内完成，
 * 因此同一响应里的健康结论、告警状态与各项计数来自同一时刻的一致快照。
 * 返回前做口径校验（计数恒等式 + 健康结论与活跃告警互相印证），
 * 校验不过直接抛错，绝不把矛盾结果悄悄返回。
 */
@Service
public class DefaultReconciliationService implements ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(DefaultReconciliationService.class);

    private final RunStateStore store;
    private final HealthEvaluationService healthEvaluationService;
    private final AlertService alertService;
    private final RejectionLogService rejectionLog;
    private final SystemClock clock;
    private final ReentrantLock lock;

    public DefaultReconciliationService(RunStateStore store,
                                        HealthEvaluationService healthEvaluationService,
                                        AlertService alertService,
                                        RejectionLogService rejectionLog,
                                        SystemClock clock,
                                        ReentrantLock observabilityLock) {
        this.store = store;
        this.healthEvaluationService = healthEvaluationService;
        this.alertService = alertService;
        this.rejectionLog = rejectionLog;
        this.clock = clock;
        this.lock = observabilityLock;
    }

    @Override
    public PipelineReconciliation reconcile(String pipelineName) {
        lock.lock();
        try {
            return reconcileLocked(pipelineName);
        } finally {
            lock.unlock();
        }
    }

    private PipelineReconciliation reconcileLocked(String pipelineName) {
        long now = clock.nowMillis();
        // 1) 先在统一时刻评估并联动告警对账：无需告警的结论会幂等关闭该管道健康类告警。
        HealthAssessment assessment = healthEvaluationService.assessAndReconcile(pipelineName, now);

        // 2) 计数快照：接收/重复/迟到来自运行状态（并发下原子累加，不丢不重）；
        //    与步骤 1 同处一个临界区，上报线程无法在中途插入新事件。
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

        // 4) 自洽校验：把校验过程与结论写进响应，调用方能核对这份结果为什么可信。
        verifyConsistency(r);
        log.info("reconciled pipeline={} at={} verdict={} activeAlerts={} received={} dup={} "
                        + "late={} rejected={} alertMerged={} consistent={}",
                pipelineName, now, assessment.getStatus(), active.size(), received, duplicates,
                late, r.getRejectedEvents(), merged, r.isConsistent());
        return r;
    }

    /**
     * 口径校验：任一校验不通过即抛出 IllegalStateException（由 web 层转成 5xx），
     * 保证调用方永远不会收到一份自相矛盾的对账结果。
     */
    private static void verifyConsistency(PipelineReconciliation r) {
        List<String> basis = new ArrayList<>();
        List<String> violations = new ArrayList<>();

        // a) 快照时刻一致：健康结论的评估时刻必须等于对账时刻。
        if (r.getEvaluatedAt() == r.getHealth().getEvaluatedAt()) {
            basis.add("snapshot-instant: health.evaluatedAt == reconciliation.evaluatedAt == "
                    + r.getEvaluatedAt());
        } else {
            violations.add("health evaluated at " + r.getHealth().getEvaluatedAt()
                    + " != reconciliation at " + r.getEvaluatedAt());
        }

        // b) 计数恒等式：接收 = 生效 + 重复 + 迟到。
        long applied = r.getReceivedEvents() - r.getDuplicateEvents() - r.getLateEvents();
        if (applied == r.getAppliedEvents() && r.getAppliedEvents() >= 0) {
            basis.add("counter-identity: received(" + r.getReceivedEvents() + ") == applied("
                    + r.getAppliedEvents() + ") + duplicate(" + r.getDuplicateEvents()
                    + ") + late(" + r.getLateEvents() + ")");
        } else {
            violations.add("counter identity broken: received=" + r.getReceivedEvents()
                    + " applied=" + r.getAppliedEvents() + " duplicate=" + r.getDuplicateEvents()
                    + " late=" + r.getLateEvents());
        }

        // c) 健康结论与活跃告警同口径：
        //    异常结论（STALE/SCHEDULE_LAGGING/SLA_BREACHED）必须有对应类型告警在挂；
        //    无需告警的结论（HEALTHY/TRANSIENT_JITTER/NEVER_RAN）不得残留健康类告警。
        AlertType expected = switch (r.getHealth().getStatus()) {
            case STALE -> AlertType.STALE;
            case SCHEDULE_LAGGING -> AlertType.SCHEDULE_LAGGING;
            case SLA_BREACHED -> AlertType.SLA_BREACHED;
            default -> null;
        };
        if (expected != null) {
            boolean present = r.getActiveAlerts().stream().anyMatch(a -> a.getType() == expected);
            if (present) {
                basis.add("health-alert-alignment: verdict=" + r.getHealth().getStatus()
                        + " has matching active alert " + expected);
            } else {
                violations.add("verdict=" + r.getHealth().getStatus()
                        + " but no active " + expected + " alert");
            }
        } else {
            List<AlertType> lingering = r.getActiveAlerts().stream()
                    .map(Alert::getType)
                    .filter(t -> t == AlertType.STALE || t == AlertType.SCHEDULE_LAGGING
                            || t == AlertType.SLA_BREACHED)
                    .toList();
            if (lingering.isEmpty()) {
                basis.add("health-alert-alignment: verdict=" + r.getHealth().getStatus()
                        + " needs no health alert and none is active");
            } else {
                violations.add("verdict=" + r.getHealth().getStatus()
                        + " but health alerts still active: " + lingering);
            }
        }

        if (!violations.isEmpty()) {
            log.error("inconsistent reconciliation for pipeline={} violations={}",
                    r.getPipelineName(), violations);
            throw new IllegalStateException("inconsistent reconciliation for pipeline "
                    + r.getPipelineName() + ": " + violations);
        }
        r.setConsistent(true);
        r.setConsistencyBasis(List.copyOf(basis));
    }
}

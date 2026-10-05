package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineReconciliation;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineRunState;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/**
 * 对账实现：先在统一时刻完成健康评估与告警对账，再快照计数，
 * 保证同一响应里的健康结论与告警状态同口径、不矛盾。
 */
@Service
public class DefaultReconciliationService implements ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(DefaultReconciliationService.class);

    private final RunStateStore store;
    private final HealthEvaluationService healthEvaluationService;
    private final AlertService alertService;
    private final RejectionLogService rejectionLog;
    private final SystemClock clock;

    public DefaultReconciliationService(RunStateStore store,
                                        HealthEvaluationService healthEvaluationService,
                                        AlertService alertService,
                                        RejectionLogService rejectionLog,
                                        SystemClock clock) {
        this.store = store;
        this.healthEvaluationService = healthEvaluationService;
        this.alertService = alertService;
        this.rejectionLog = rejectionLog;
        this.clock = clock;
    }

    @Override
    public PipelineReconciliation reconcile(String pipelineName) {
        long now = clock.nowMillis();
        // 1) 先在统一时刻评估并联动告警对账：HEALTHY 会幂等关闭该管道健康类告警。
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
        log.info("reconciled pipeline={} at={} verdict={} activeAlerts={} received={} dup={} "
                        + "late={} rejected={} alertMerged={}",
                pipelineName, now, assessment.getStatus(), active.size(), received, duplicates,
                late, r.getRejectedEvents(), merged);
        return r;
    }
}

package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineMetrics;
import com.github.xingjianluo.pipelineobservabilityjava.model.ReconciliationReport;
import com.github.xingjianluo.pipelineobservabilityjava.store.PipelineMetricsRegistry;
import com.github.xingjianluo.pipelineobservabilityjava.store.RejectionLedger;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;

/**
 * 运行质量与上报告警对账。
 * 关键：健康判定、告警对账、告警快照、计数快照在“同一管道同一把锁”内依次取齐，
 * 因此同一时刻返回的健康结论与告警状态必然同一口径、互不矛盾。
 */
@Service
public class DefaultReconciliationService implements ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(DefaultReconciliationService.class);

    private final HealthEvaluationService healthEvaluationService;
    private final AlertService alertService;
    private final RunStateStore runStateStore;
    private final RejectionLedger rejectionLedger;
    private final PipelineMetricsRegistry metricsRegistry;

    /** 每管道一把对账锁：串行化“评估->对账->取快照”，保证结论与告警口径一致。 */
    private final ConcurrentMap<String, Object> locks = new ConcurrentHashMap<>();

    public DefaultReconciliationService(HealthEvaluationService healthEvaluationService,
                                        AlertService alertService,
                                        RunStateStore runStateStore,
                                        RejectionLedger rejectionLedger,
                                        PipelineMetricsRegistry metricsRegistry) {
        this.healthEvaluationService = healthEvaluationService;
        this.alertService = alertService;
        this.runStateStore = runStateStore;
        this.rejectionLedger = rejectionLedger;
        this.metricsRegistry = metricsRegistry;
    }

    @Override
    public ReconciliationReport report(String pipelineName, long atMillis) {
        Object lock = locks.computeIfAbsent(pipelineName, k -> new Object());
        synchronized (lock) {
            // 1) 健康判定并立即对账告警（触发/恢复在同一临界区完成）。
            HealthAssessment assessment = healthEvaluationService.assessAndReconcile(pipelineName, atMillis);

            // 2) 对账后立刻取活跃告警快照——与上面的健康结论是同一口径。
            List<Alert> active = alertService.list(false).stream()
                    .filter(a -> a.getPipelineName().equals(pipelineName))
                    .toList();

            ReconciliationReport r = new ReconciliationReport();
            r.setPipelineName(pipelineName);
            r.setEvaluatedAt(atMillis);
            r.setHealth(assessment);
            r.setHealthStatus(assessment.getStatus());
            r.getActiveAlerts().addAll(active);

            // 3) 接入计数与拒绝原因分布（同一时刻快照）。
            PipelineMetrics metrics = metricsRegistry.snapshot(pipelineName);
            r.setMetrics(metrics);
            r.getRejectedByReason().putAll(rejectionLedger.countByReason(pipelineName));

            // 4) 告警合并/抑制/通知计数（活跃告警聚合）。
            r.setAlertMergedCount(alertService.mergedCountForPipeline(pipelineName));
            r.setAlertSuppressedCount(alertService.suppressedCountForPipeline(pipelineName));
            r.setAlertNotifiedCount(alertService.notifiedCountForPipeline(pipelineName));

            // 5) 自洽性校验：健康结论与活跃告警必须互相印证。
            checkConsistency(r, assessment, active);
            log.info("reconciliation pipeline={} at={} health={} activeAlerts={} consistent={} metrics=[recv={},acc={},dup={},late={},rej={}]",
                    pipelineName, atMillis, assessment.getStatus(), active.size(), r.isConsistent(),
                    metrics.getReceived(), metrics.getAccepted(), metrics.getDuplicate(),
                    metrics.getLate(), metrics.getRejected());
            return r;
        }
    }

    @Override
    public List<ReconciliationReport> reportAll(long atMillis) {
        // 管道集合：已有运行的管道 + 已计数（含被拒）的管道，按名排序，逐管道在其锁内对账。
        java.util.TreeSet<String> names = new java.util.TreeSet<>();
        runStateStore.findAll().forEach(s -> {
            if (s.getPipelineName() != null) {
                names.add(s.getPipelineName());
            }
        });
        metricsRegistry.snapshots().forEach(m -> {
            if (m.getPipelineName() != null
                    && !com.github.xingjianluo.pipelineobservabilityjava.store.InMemoryPipelineMetricsRegistry.UNKNOWN_PIPELINE
                    .equals(m.getPipelineName())) {
                names.add(m.getPipelineName());
            }
        });
        return names.stream().map(n -> report(n, atMillis)).toList();
    }

    /**
     * 结论自洽规则：
     * - HEALTHY：该管道不得残留任何活跃健康类/失败类告警；
     * - STALE/SCHEDULE_LAGGING/SLA_BREACHED：必须存在同类型的活跃告警；
     * - NEVER_RAN/TRANSIENT_JITTER：非健康但属预期的“无正式告警”态，允许无活跃告警；
     * - RUN_FAILED 不来自健康判定，若存在活跃 RUN_FAILED 而健康为 HEALTHY 也判矛盾。
     */
    private void checkConsistency(ReconciliationReport r, HealthAssessment a, List<Alert> active) {
        boolean ok = true;
        Set<com.github.xingjianluo.pipelineobservabilityjava.model.AlertType> activeTypes =
                new java.util.HashSet<>();
        active.forEach(x -> activeTypes.add(x.getType()));
        FreshnessStatus s = a.getStatus();

        if (s == FreshnessStatus.HEALTHY) {
            if (!active.isEmpty()) {
                ok = false;
                r.getConsistencyChecks().add(
                        "CONTRADICTION: health=HEALTHY but active alerts remain: " + activeTypes);
            } else {
                r.getConsistencyChecks().add("OK: HEALTHY with no active alerts");
            }
        } else if (s == FreshnessStatus.STALE
                || s == FreshnessStatus.SCHEDULE_LAGGING
                || s == FreshnessStatus.SLA_BREACHED) {
            com.github.xingjianluo.pipelineobservabilityjava.model.AlertType expect = switch (s) {
                case STALE -> com.github.xingjianluo.pipelineobservabilityjava.model.AlertType.STALE;
                case SCHEDULE_LAGGING ->
                        com.github.xingjianluo.pipelineobservabilityjava.model.AlertType.SCHEDULE_LAGGING;
                case SLA_BREACHED ->
                        com.github.xingjianluo.pipelineobservabilityjava.model.AlertType.SLA_BREACHED;
                default -> throw new IllegalStateException();
            };
            if (!activeTypes.contains(expect)) {
                ok = false;
                r.getConsistencyChecks().add(
                        "CONTRADICTION: health=" + s + " but no active " + expect + " alert");
            } else {
                r.getConsistencyChecks().add("OK: " + s + " backed by active " + expect + " alert");
            }
        } else {
            r.getConsistencyChecks().add("OK: " + s + " is a non-alerting expected state, activeTypes="
                    + activeTypes);
        }
        r.setConsistent(ok);
    }
}

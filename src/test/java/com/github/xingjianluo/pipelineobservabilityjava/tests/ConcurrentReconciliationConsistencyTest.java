package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineReconciliation;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.ReconciliationService;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/**
 * 固定服务时间下，上报与对账高并发交错：每一份对账结果都必须是同一时刻的一致快照——
 * consistent 标志为真、计数恒等式成立、健康结论与活跃告警同口径、评估时刻等于固定时钟，
 * 绝不允许出现结论与告警互相矛盾的组合。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ConcurrentReconciliationConsistencyTest {

    private static final Logger log =
            LoggerFactory.getLogger(ConcurrentReconciliationConsistencyTest.class);
    private static final long T0 = ClockTestConfig.T0;
    private static final String P = "conc-recon";
    private static final int THREADS = 8;
    private static final int PER_THREAD = 40;
    private static final int RUNS = 8;

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private ReconciliationService reconciliation;
    @Autowired
    private SystemClock clock;

    private void send(String json) {
        try {
            ingestion.ingest(mapper.readTree(json));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String ev(String runId, String status, long t) {
        return "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\"" + P
                + "\",\"status\":\"" + status + "\",\"eventTime\":" + t + "}";
    }

    @Test
    void concurrentIngestAndReconcileAlwaysConsistent() throws Exception {
        // 时钟全程固定在 T0：上报联动与对账必须落在同一时刻，任何快照都自洽。
        ((MutableClock) clock).set(T0);
        log.info("[INPUT] fixed clock T0={} threads={} perThread={} runs={}", T0, THREADS, PER_THREAD, RUNS);

        AtomicBoolean ingesting = new AtomicBoolean(true);
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        ExecutorService auditors = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; i++) {
            auditors.submit(() -> {
                while (ingesting.get()) {
                    PipelineReconciliation r = reconciliation.reconcile(P);
                    if (!r.isConsistent()) {
                        violations.add("consistent flag false: " + r.getConsistencyBasis());
                    }
                    if (r.getEvaluatedAt() != T0 || r.getHealth().getEvaluatedAt() != T0) {
                        violations.add("snapshot not at fixed clock: evaluatedAt=" + r.getEvaluatedAt()
                                + " healthAt=" + r.getHealth().getEvaluatedAt());
                    }
                    if (r.getReceivedEvents()
                            != r.getAppliedEvents() + r.getDuplicateEvents() + r.getLateEvents()) {
                        violations.add("counter identity broken: " + r.getReceivedEvents() + " != "
                                + r.getAppliedEvents() + "+" + r.getDuplicateEvents()
                                + "+" + r.getLateEvents());
                    }
                    boolean expectNoHealthAlert = switch (r.getHealth().getStatus()) {
                        case HEALTHY, NEVER_RAN, TRANSIENT_JITTER -> true;
                        default -> false;
                    };
                    long healthAlerts = r.getActiveAlerts().stream()
                            .filter(a -> a.getType() == AlertType.STALE
                                    || a.getType() == AlertType.SCHEDULE_LAGGING
                                    || a.getType() == AlertType.SLA_BREACHED)
                            .count();
                    if (expectNoHealthAlert && healthAlerts > 0) {
                        violations.add("verdict=" + r.getHealth().getStatus()
                                + " but health alerts active=" + healthAlerts);
                    }
                    if (!expectNoHealthAlert && healthAlerts == 0) {
                        violations.add("verdict=" + r.getHealth().getStatus()
                                + " but no matching health alert active");
                    }
                }
            });
        }

        // 上报风暴：RUNNING+SUCCESS（大量精确重复）夹带非法事件（被拒）。
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        for (int th = 0; th < THREADS; th++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < PER_THREAD; i++) {
                        String runId = "cr-" + (i % RUNS);
                        send(ev(runId, "RUNNING", T0 - 2_000L));
                        send(ev(runId, "SUCCESS", T0 - 1_000L));
                        send(ev(runId, "BOGUS", T0 - 1_000L));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers should finish");
        ingesting.set(false);
        auditors.shutdown();
        assertTrue(auditors.awaitTermination(30, TimeUnit.SECONDS), "auditors should finish");

        PipelineReconciliation r = reconciliation.reconcile(P);
        log.info("[RECON] verdict={} activeAlerts={} received={} dup={} late={} applied={} "
                        + "rejected={} consistent={} basis={}",
                r.getHealth().getStatus(), r.getActiveAlerts().size(), r.getReceivedEvents(),
                r.getDuplicateEvents(), r.getLateEvents(), r.getAppliedEvents(),
                r.getRejectedEvents(), r.isConsistent(), r.getConsistencyBasis());
        // 8 runs × (1 RUNNING + 1 SUCCESS) 生效，其余 8×(40-1)×2 重复；被拒 8×40。
        assertEquals(8 * 2 * PER_THREAD, r.getReceivedEvents());
        assertEquals(8 * 2 * (PER_THREAD - 1), r.getDuplicateEvents());
        assertEquals(16, r.getAppliedEvents());
        assertEquals(8 * PER_THREAD, r.getRejectedEvents());
        assertEquals(FreshnessStatus.HEALTHY, r.getHealth().getStatus());
        assertEquals(0, r.getActiveAlerts().size());
        assertTrue(r.isConsistent());
        assertTrue(violations.isEmpty(), "concurrent reconciliation violations: " + violations);
        log.info("[ASSERT] {} concurrent snapshots all self-consistent at fixed clock", "every");
    }
}

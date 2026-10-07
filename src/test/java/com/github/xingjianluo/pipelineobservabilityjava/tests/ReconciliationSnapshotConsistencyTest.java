package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineReconciliation;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.ReconciliationService;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/**
 * 上报与对账并发打进来时，每一份对账都必须是同一时刻的一致快照：
 * consistent=true、健康结论与活跃告警同口径、各类计数勾稽得上。
 * 任何一个线程拿到矛盾组合都算失败（矛盾绝不悄悄返回）。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
class ReconciliationSnapshotConsistencyTest {

    private static final Logger log =
            LoggerFactory.getLogger(ReconciliationSnapshotConsistencyTest.class);
    private static final long T0 = ClockTestConfig.T0;
    private static final String HEALTHY_P = "snap-healthy";
    private static final String STALE_P = "snap-stale";
    private static final int WRITERS = 4;
    private static final int WRITES_PER_WRITER = 150;
    private static final int READERS = 3;
    private static final int READS_PER_READER = 80;

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private ReconciliationService reconciliation;
    @Autowired
    private SystemClock clock;

    private void send(String pipeline, String runId, String status, long eventTime)
            throws Exception {
        ingestion.ingest(mapper.readTree("{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId
                + "\",\"pipelineName\":\"" + pipeline + "\",\"status\":\"" + status
                + "\",\"eventTime\":" + eventTime + "}"));
    }

    /** 对一份对账做自洽判定，返回矛盾点列表（空=自洽）。 */
    private List<String> check(PipelineReconciliation r) {
        List<String> problems = new java.util.ArrayList<>();
        if (!r.isConsistent()) {
            problems.add("consistent flag is false");
        }
        if (r.getAppliedEvents()
                != r.getReceivedEvents() - r.getDuplicateEvents() - r.getLateEvents()) {
            problems.add("counter identity broken: applied=" + r.getAppliedEvents());
        }
        long rejectedSum = r.getRejectionsByCode().values().stream()
                .mapToLong(Long::longValue).sum();
        if (rejectedSum != r.getRejectedEvents()) {
            problems.add("rejection identity broken: total=" + r.getRejectedEvents()
                    + " vs byCodeSum=" + rejectedSum);
        }
        FreshnessStatus v = r.getHealth().getStatus();
        boolean staleActive = r.getActiveAlerts().stream()
                .anyMatch(a -> a.getType() == AlertType.STALE);
        if (v == FreshnessStatus.HEALTHY && !r.getActiveAlerts().isEmpty()) {
            problems.add("HEALTHY verdict coexists with active alerts "
                    + r.getActiveAlerts().stream().map(a -> a.getType().name()).toList());
        }
        if (v == FreshnessStatus.STALE && !staleActive) {
            problems.add("STALE verdict without an active STALE alert");
        }
        if (r.getEvaluatedAt() != r.getHealth().getEvaluatedAt()) {
            problems.add("health evaluated at a different instant than the reconciliation");
        }
        return problems;
    }

    @Test
    void concurrentIngestAndReconcileNeverYieldContradictorySnapshots() throws Exception {
        ((MutableClock) clock).set(T0);
        // 确定性前置状态：snap-healthy 新鲜（lastSuccess=T0-1000），snap-stale 超期（T0-4.9M）
        send(HEALTHY_P, "snap-h-0", "SUCCESS", T0 - 1_000L);
        send(STALE_P, "snap-s-0", "SUCCESS", T0 - 4_900_000L);
        log.info("[SETUP] clock fixed at T0={}; {} fresh, {} stale; writers={}x{} readers={}x{}",
                T0, HEALTHY_P, STALE_P, WRITERS, WRITES_PER_WRITER, READERS, READS_PER_READER);

        ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> samples = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(WRITERS + READERS);
        CountDownLatch ready = new CountDownLatch(WRITERS + READERS);
        CountDownLatch go = new CountDownLatch(1);

        // 写线程：新鲜管道反复上报新成功（推进状态）+ 精确重复；超期管道上报重复事件
        for (int w = 0; w < WRITERS; w++) {
            int id = w;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int i = 0; i < WRITES_PER_WRITER; i++) {
                        // 交替：推进状态的新成功 / 精确重复 / 被拒（未知状态）
                        send(HEALTHY_P, "snap-h-" + id + "-" + i, "SUCCESS", T0 - 900L);
                        send(HEALTHY_P, "snap-h-" + id + "-" + i, "SUCCESS", T0 - 900L);
                        send(STALE_P, "snap-s-0", "SUCCESS", T0 - 4_900_000L);
                        if (i % 10 == 0) {
                            send(HEALTHY_P, "snap-bad-" + id + "-" + i, "BOGUS", T0 - 800L);
                        }
                    }
                } catch (Exception e) {
                    failures.add("writer " + id + " failed: " + e);
                }
            });
        }
        // 读线程：持续对账两个管道，每份结果都必须自洽
        for (int r = 0; r < READERS; r++) {
            int id = r;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int i = 0; i < READS_PER_READER; i++) {
                        for (String p : List.of(HEALTHY_P, STALE_P)) {
                            PipelineReconciliation rec = reconciliation.reconcile(p);
                            List<String> problems = check(rec);
                            if (!problems.isEmpty()) {
                                failures.add("reader " + id + " pipeline=" + p
                                        + " contradictory snapshot: " + problems);
                            } else if (i == 0) {
                                samples.add("reader " + id + " pipeline=" + p
                                        + " verdict=" + rec.getHealth().getStatus()
                                        + " activeAlerts=" + rec.getActiveAlerts().size()
                                        + " notes=" + rec.getConsistencyNotes());
                            }
                        }
                    }
                } catch (Exception e) {
                    failures.add("reader " + id + " failed: " + e);
                }
            });
        }
        assertTrue(ready.await(30, TimeUnit.SECONDS));
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS));

        samples.forEach(s -> log.info("[SAMPLE] {}", s));
        log.info("[RESULT] concurrent snapshots checked={} contradictions={}",
                READERS * READS_PER_READER * 2, failures.size());
        assertTrue(failures.isEmpty(),
                "no contradictory reconciliation may be returned: " + failures);

        // 收尾：终态对账必须自洽且计数精确
        PipelineReconciliation h = reconciliation.reconcile(HEALTHY_P);
        PipelineReconciliation s = reconciliation.reconcile(STALE_P);
        log.info("[FINAL] {} verdict={} received={} dup={} applied={} rejected={} consistent={}",
                HEALTHY_P, h.getHealth().getStatus(), h.getReceivedEvents(), h.getDuplicateEvents(),
                h.getAppliedEvents(), h.getRejectedEvents(), h.isConsistent());
        log.info("[FINAL] {} verdict={} received={} activeAlerts={} consistent={}",
                STALE_P, s.getHealth().getStatus(), s.getReceivedEvents(),
                s.getActiveAlerts().size(), s.isConsistent());
        assertTrue(h.isConsistent() && s.isConsistent());
        assertEquals(FreshnessStatus.HEALTHY, h.getHealth().getStatus());
        assertEquals(FreshnessStatus.STALE, s.getHealth().getStatus());
        // 每个写线程每轮：1 新成功 + 1 重复 + 每 10 轮 1 被拒；超期管道每轮 1 重复
        long expectedReceived = 1 + WRITERS * WRITES_PER_WRITER * 2L;
        assertEquals(expectedReceived, h.getReceivedEvents());
        assertEquals(WRITERS * WRITES_PER_WRITER, h.getDuplicateEvents());
        assertEquals(WRITERS * (WRITES_PER_WRITER / 10), h.getRejectedEvents());
        // 超期管道：前置事件为 APPLY，之后每轮同样的成功事件均为精确重复
        assertEquals(WRITERS * WRITES_PER_WRITER, s.getDuplicateEvents());
    }
}

package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
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
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineReconciliation;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineRunState;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.ReconciliationService;
import com.github.xingjianluo.pipelineobservabilityjava.service.RejectionLogService;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/**
 * 并发上报下的精确计数：大量线程同时打入合法、重复、迟到、非法事件，
 * 接收/重复/迟到/被拒计数必须精确不丢不重；并发对账快照时刻满足恒等式
 * received == applied + duplicate + late，且健康结论与活跃告警同口径。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ConcurrentExactCountTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentExactCountTest.class);
    private static final String P = "conc-exact";
    private static final int RUNS = 10;
    private static final int THREADS = 8;
    private static final int PER_THREAD = 50;
    private static final int LATE_PER_THREAD = 25;

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private RunStateStore store;
    @Autowired
    private RejectionLogService rejectionLog;
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
    void concurrentMixedTrafficCountsExactly() throws Exception {
        // 事件时间锚定真实当前时刻，测试时钟拨到同一基准：
        // 接入时的联动对账（真实时钟）与显式对账（测试时钟）口径一致，不互相干扰。
        long base = System.currentTimeMillis();
        ((MutableClock) clock).set(base);
        long t1 = base + 1_000L;
        long t2 = base + 2_000L;
        log.info("[INPUT] base={} threads={} perThread={} runs={}", base, THREADS, PER_THREAD, RUNS);

        // 两个对账线程在上报风暴期间持续取快照，验证恒等式与口径自洽（独立线程池）。
        AtomicBoolean ingesting = new AtomicBoolean(true);
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        ExecutorService auditors = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; i++) {
            auditors.submit(() -> {
                while (ingesting.get()) {
                    PipelineReconciliation r = reconciliation.reconcile(P);
                    if (r.getReceivedEvents()
                            != r.getAppliedEvents() + r.getDuplicateEvents() + r.getLateEvents()) {
                        violations.add("invariant broken: " + r.getReceivedEvents() + " != "
                                + r.getAppliedEvents() + "+" + r.getDuplicateEvents()
                                + "+" + r.getLateEvents());
                    }
                    // 口径自洽：正式告警状态（STALE/SCHEDULE_LAGGING/SLA_BREACHED）必有活跃告警；
                    // HEALTHY/NEVER_RAN/TRANSIENT_JITTER 不应挂任何活跃告警。
                    boolean expectNoAlert = switch (r.getHealth().getStatus()) {
                        case HEALTHY, NEVER_RAN, TRANSIENT_JITTER -> true;
                        default -> false;
                    };
                    if (expectNoAlert != r.getActiveAlerts().isEmpty()) {
                        violations.add("health/alert mismatch: verdict="
                                + r.getHealth().getStatus() + " activeAlerts="
                                + r.getActiveAlerts().size());
                    }
                }
            });
        }

        // 阶段一：8 线程 × 50 轮，每轮对 10 个 run 之一发 RUNNING+SUCCESS（大量精确重复）
        // 并夹带 1 条非法事件（未知状态 -> 被拒）。
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        for (int th = 0; th < THREADS; th++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < PER_THREAD; i++) {
                        String runId = "cx-" + (i % RUNS);
                        send(ev(runId, "RUNNING", t1));
                        send(ev(runId, "SUCCESS", t2));
                        send(ev(runId, "BOGUS", t2)); // 未知状态 -> UNKNOWN_STATUS 被拒
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers should finish");

        // 阶段二：终结态确定后，再并发打入 8×25 条更旧的 PENDING -> 全部迟到忽略。
        ExecutorService latePool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start2 = new CountDownLatch(1);
        for (int th = 0; th < THREADS; th++) {
            latePool.submit(() -> {
                try {
                    start2.await();
                    for (int i = 0; i < LATE_PER_THREAD; i++) {
                        send(ev("cx-" + (i % RUNS), "PENDING", t1 - 500L));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start2.countDown();
        latePool.shutdown();
        assertTrue(latePool.awaitTermination(60, TimeUnit.SECONDS), "late workers should finish");
        ingesting.set(false);
        auditors.shutdown();
        assertTrue(auditors.awaitTermination(30, TimeUnit.SECONDS), "auditors should finish");

        // 精确计数：每 run 80 有效事件（40 RUNNING + 40 SUCCESS）= 2 生效 + 78 重复，
        // 全局 800 有效 + 200 迟到 + 400 被拒。
        List<PipelineRunState> runs = store.findAll().stream()
                .filter(s -> P.equals(s.getPipelineName())).toList();
        long received = runs.stream().mapToLong(PipelineRunState::getEventCount).sum();
        long dup = runs.stream().mapToLong(PipelineRunState::getDuplicateCount).sum();
        long late = runs.stream().mapToLong(PipelineRunState::getLateIgnoredCount).sum();
        log.info("[EXACT] runs={} received={} dup={} late={} applied={} rejected={}",
                runs.size(), received, dup, late, received - dup - late,
                rejectionLog.totalCount(P));
        assertEquals(RUNS, runs.size());
        assertEquals(1000, received);
        assertEquals(780, dup);
        assertEquals(200, late);
        assertEquals(20, received - dup - late);
        assertEquals(400, rejectionLog.totalCount(P));
        assertEquals(400L, rejectionLog.countByCode(P).get(RejectCode.UNKNOWN_STATUS));

        // 对账视图与底层计数完全一致，且结论口径自洽。
        PipelineReconciliation r = reconciliation.reconcile(P);
        log.info("[RECON] verdict={} activeAlerts={} received={} dup={} late={} applied={} "
                        + "rejected={} byCode={}", r.getHealth().getStatus(),
                r.getActiveAlerts().size(), r.getReceivedEvents(), r.getDuplicateEvents(),
                r.getLateEvents(), r.getAppliedEvents(), r.getRejectedEvents(),
                r.getRejectionsByCode());
        assertEquals(1000, r.getReceivedEvents());
        assertEquals(780, r.getDuplicateEvents());
        assertEquals(200, r.getLateEvents());
        assertEquals(20, r.getAppliedEvents());
        assertEquals(400, r.getRejectedEvents());
        assertEquals(FreshnessStatus.HEALTHY, r.getHealth().getStatus());
        assertEquals(0, r.getActiveAlerts().size());

        assertTrue(violations.isEmpty(), "concurrent reconciliation violations: " + violations);
        log.info("[ASSERT] no invariant violation during concurrent reconciliation");
    }
}

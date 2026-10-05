package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineMetrics;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.store.PipelineMetricsRegistry;
import com.github.xingjianluo.pipelineobservabilityjava.store.RejectionLedger;

/**
 * 大量线程同时上报（含接受/重复/迟到/被拒四类）：
 * 计数不丢不重，且 received 恒等于 accepted+duplicate+late+rejected；
 * 多个管道的计数互不串台；对账快照始终守恒。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ConcurrentMetricsTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentMetricsTest.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private PipelineMetricsRegistry metrics;
    @Autowired
    private RejectionLedger ledger;

    private JsonNode event(String pipeline, String runId, String status, long t) throws Exception {
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\""
                + pipeline + "\",\"status\":\"" + status + "\",\"eventTime\":" + t + "}";
        return mapper.readTree(json);
    }

    private JsonNode rejectEvent(String pipeline) throws Exception {
        // 非法时间 => INVALID_TIMESTAMP
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"bad\",\"pipelineName\":\""
                + pipeline + "\",\"status\":\"RUNNING\",\"eventTime\":-1}";
        return mapper.readTree(json);
    }

    @Test
    void concurrentReportsYieldExactBalancedCounts() throws Exception {
        String p = "cm-main";
        int threads = 16;
        int perThread = 200; // 每个线程：1 条终态应用 + 198 重复 + 少量迟到/拒绝，见下
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger();

        List<Long> distinctSuccessTimes = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            distinctSuccessTimes.add(1_700_000_100_000L + i * 1000L);
        }

        for (int th = 0; th < threads; th++) {
            final int idx = th;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        JsonNode node;
                        int kind = i % 10;
                        if (kind == 0) {
                            // 40 个不同时间的 SUCCESS：终结覆盖，按 (status,eventTime) 去重
                            long t = distinctSuccessTimes.get((idx + i) % distinctSuccessTimes.size());
                            node = event(p, "cm-run", "SUCCESS", t);
                        } else if (kind == 1) {
                            // 很旧的 RUNNING：终态后必为迟到
                            node = event(p, "cm-run", "RUNNING", 1_699_999_000_000L + i);
                        } else if (kind == 2) {
                            // 被拒
                            node = rejectEvent(p);
                        } else {
                            // 其余：同一条重复 SUCCESS
                            node = event(p, "cm-run", "SUCCESS",
                                    distinctSuccessTimes.get(0));
                        }
                        ingestion.ingest(node);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    log.error("worker error", e);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers finish");
        assertEquals(0, errors.get());

        PipelineMetrics m = metrics.snapshot(p);
        long total = threads * perThread;
        long parts = m.getAccepted() + m.getDuplicate() + m.getLate() + m.getRejected();
        log.info("[METRICS] received={} accepted={} duplicate={} late={} rejected={} sum={}",
                m.getReceived(), m.getAccepted(), m.getDuplicate(), m.getLate(),
                m.getRejected(), parts);
        assertEquals(total, m.getReceived(), "no report may be lost");
        assertEquals(total, parts, "received must equal accepted+duplicate+late+rejected");

        Map<RejectCode, Long> byReason = ledger.countByReason(p);
        log.info("[REJECT STATS] {}", byReason);
        // kind==2：每线程 200 条中 20 条被拒
        assertEquals(threads * 20L, m.getRejected());
        assertEquals(threads * 20L, byReason.get(RejectCode.INVALID_TIMESTAMP));

        // 再取一次快照，数字稳定不变（无并发写入后快照确定性）
        PipelineMetrics m2 = metrics.snapshot(p);
        assertEquals(m.getReceived(), m2.getReceived());
        assertEquals(parts, m2.getAccepted() + m2.getDuplicate() + m2.getLate() + m2.getRejected());
        log.info("[STABLE SNAPSHOT] received={} balanced=true", m2.getReceived());
    }

    @Test
    void twoPipelinesCountersAreIsolatedUnderConcurrency() throws Exception {
        String p1 = "cm-a";
        String p2 = "cm-b";
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        for (int th = 0; th < 8; th++) {
            final String target = (th % 2 == 0) ? p1 : p2;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 100; i++) {
                        ingestion.ingest(event(target, "r-" + target,
                                "SUCCESS", 1_700_000_200_000L + (i % 5)));
                    }
                } catch (Exception e) {
                    log.error("worker error", e);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        PipelineMetrics a = metrics.snapshot(p1);
        PipelineMetrics b = metrics.snapshot(p2);
        log.info("[ISOLATED] a.recv={} b.recv={} a.dup={} b.dup={}",
                a.getReceived(), b.getReceived(), a.getDuplicate(), b.getDuplicate());
        assertEquals(400, a.getReceived());
        assertEquals(400, b.getReceived());
        assertEquals(0, a.getRejected());
        assertEquals(0, b.getRejected());
        assertEquals(a.getReceived(), a.getAccepted() + a.getDuplicate() + a.getLate());
        assertEquals(b.getReceived(), b.getAccepted() + b.getDuplicate() + b.getLate());
    }
}

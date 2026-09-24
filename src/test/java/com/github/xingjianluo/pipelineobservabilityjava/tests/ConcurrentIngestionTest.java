package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertState;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.RunStatus;
import com.github.xingjianluo.pipelineobservabilityjava.service.AlertService;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;

/** 高并发上报：计数不丢失、状态确定唯一、重复告警合并为一条。 */
@SpringBootTest
@Import(ClockTestConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ConcurrentIngestionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestionTest.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private RunStateStore store;
    @Autowired
    private AlertService alerts;

    private String event(String runId, String status, long t) {
        return "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\"conc\","
                + "\"status\":\"" + status + "\",\"eventTime\":" + t + "}";
    }

    @Test
    void concurrentDuplicatesAndFailuresConvergeConsistently() throws Exception {
        int threads = 16;
        int perThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger();
        List<Long> runningTimes = new ArrayList<>();
        for (int i = 0; i < perThread; i++) {
            runningTimes.add(1_700_000_001_000L + i);
        }

        for (int th = 0; th < threads; th++) {
            final int base = th;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // 一半线程发重复的同一条 RUNNING，另一半发同一条 FAILED（终结）。
                        String json = (base % 2 == 0)
                                ? event("conc-1", "RUNNING", 1_700_000_001_000L)
                                : event("conc-1", "FAILED", 1_700_000_009_000L);
                        ingestion.ingest(mapper.readTree(json));
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    log.error("worker error", e);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "workers should finish");
        assertEquals(0, errors.get());

        var state = store.find("conc-1").orElseThrow();
        log.info("[CONVERGED] status={} eventCount={} duplicateCount={} lateIgnored={} version={}",
                state.getStatus(), state.getEventCount(), state.getDuplicateCount(),
                state.getLateIgnoredCount(), state.getVersion());
        // 结论唯一确定：FAILED 终结，绝不因并发而停在 RUNNING。
        assertEquals(RunStatus.FAILED, state.getStatus());
        // 1600 个事件必须全部被计数（精确重复计入 duplicate），不丢失。
        assertEquals(threads * perThread, state.getEventCount());
        long distinctApplied = state.getEventCount()
                - state.getDuplicateCount() - state.getLateIgnoredCount();
        log.info("[ASSERT] applied(non-dup,non-late)={}", distinctApplied);
        assertTrue(distinctApplied >= 1, "at least the terminal event applies");

        // 同一 (pipeline,type) 告警只有一条，count 反映失败观测的合并次数。
        List<Alert> active = alerts.list(false);
        long failAlerts = active.stream()
                .filter(a -> a.getType() == AlertType.RUN_FAILED && "conc".equals(a.getPipelineName()))
                .count();
        assertEquals(1, failAlerts, "failures must merge into one alert");
        Alert fail = active.stream()
                .filter(a -> a.getType() == AlertType.RUN_FAILED && "conc".equals(a.getPipelineName()))
                .findFirst().orElseThrow();
        assertEquals(AlertState.FIRING, fail.getState());
        log.info("[ASSERT] one RUN_FAILED alert count={} severity={}", fail.getCount(), fail.getSeverity());
    }
}

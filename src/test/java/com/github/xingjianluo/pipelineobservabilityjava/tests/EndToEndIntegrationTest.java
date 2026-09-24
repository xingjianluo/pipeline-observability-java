package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.github.xingjianluo.pipelineobservabilityjava.observability.store.AlertStore;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.PipelineRegistry;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.RunStateStore;
import com.github.xingjianluo.pipelineobservabilityjava.observability.util.MutableClock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

/**
 * 端到端：HTTP 接入 -> 状态收敛 -> 健康判定 -> 告警聚合，覆盖乱序/并发/边界/拒绝留痕。
 */
@SpringBootTest(properties = {
        "pipeline.observability.scheduler-enabled=false",
        "pipeline.observability.defaults.freshness-threshold-ms=300000",
        "pipeline.observability.defaults.never-succeeded-grace-ms=200000",
        "pipeline.observability.defaults.jitter-grace-ms=60000",
        "pipeline.observability.defaults.sla-run-duration-ms=120000",
        "pipeline.observability.pipelines.scheduled.schedule-interval-ms=60000",
        "pipeline.observability.pipelines.scheduled.schedule-tolerance-ms=30000"
})
@AutoConfigureMockMvc
class EndToEndIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(EndToEndIntegrationTest.class);

    @Autowired private MockMvc mockMvc;
    @Autowired private MutableClock clock;
    @Autowired private RunStateStore runStateStore;
    @Autowired private PipelineRegistry pipelineRegistry;
    @Autowired private AlertStore alertStore;

    private String event(String pipeline, String run, String type, long at, Long deadline) {
        String extra = deadline != null ? ",\"slaDeadlineAt\":" + deadline : "";
        return "{\"protocolVersion\":\"1.0\",\"pipelineId\":\"" + pipeline
                + "\",\"runId\":\"" + run + "\",\"eventType\":\"" + type
                + "\",\"occurredAt\":" + at + extra + "}";
    }

    @Test
    void fullLifecycleWithOutOfOrderAndRejections() throws Exception {
        clock.setNow(100_000L);
        // 1) 非法事件不静默丢弃：400 + MALFORMED_JSON，并进入拒绝留痕
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content("oops"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("MALFORMED_JSON"));
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"protocolVersion\":\"9.9\",\"pipelineId\":\"p\",\"runId\":\"r\","
                                + "\"eventType\":\"HEARTBEAT\",\"occurredAt\":1}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("UNSUPPORTED_PROTOCOL_VERSION"));
        mockMvc.perform(get("/api/events/rejections"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
        log.info("[端到端拒绝] 2 条非法事件均被区分拒绝并留痕");

        // 2) 正常运行：started -> heartbeat -> succeeded
        mockMvc.perform(post("/api/events").content(event("orders", "r1", "RUN_STARTED", 100_000, null)))
                .andExpect(status().isOk()).andExpect(jsonPath("$.outcome").value("ACCEPTED"));
        mockMvc.perform(post("/api/events").content(event("orders", "r1", "HEARTBEAT", 130_000, null)))
                .andExpect(status().isOk());
        // 重复心跳（producer 重试）幂等
        mockMvc.perform(post("/api/events").content(event("orders", "r1", "HEARTBEAT", 130_000, null)))
                .andExpect(status().isOk()).andExpect(jsonPath("$.outcome").value("DUPLICATE_IGNORED"));
        mockMvc.perform(post("/api/events").content(event("orders", "r1", "SUCCEEDED", 160_000, null)))
                .andExpect(status().isOk());
        // 终结后迟到事件不回退
        mockMvc.perform(post("/api/events").content(event("orders", "r1", "FAILED", 170_000, null)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("LATE_IGNORED_TERMINAL"))
                .andExpect(jsonPath("$.runStatus").value("SUCCEEDED"));
        log.info("[端到端收敛] orders/r1 重复心跳幂等，迟到 FAILED 未回退 SUCCEEDED");

        // 3) 新鲜度边界：300s 阈值。成功在 160000 -> at=460000 边界相等 HEALTHY；460001 STALE
        clock.setNow(460_000L);
        mockMvc.perform(post("/api/health/tick"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orders.status").value("HEALTHY"));
        clock.setNow(460_001L);
        mockMvc.perform(post("/api/health/tick"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orders.status").value("STALE"));
        log.info("[端到端新鲜度] age=300000 边界 HEALTHY，age=300001 STALE 并触发 FRESHNESS 告警");

        // 4) 告警去重：再 tick 一轮只合并计数，TRIGGERED 仍 1 条
        clock.setNow(470_000L);
        mockMvc.perform(post("/api/health/tick")).andExpect(status().isOk());
        long triggered = alertStore.notifications().stream()
                .filter(n -> n.kind().name().equals("TRIGGERED")
                        && n.pipelineId().equals("orders")).count();
        AlertRecordView v = AlertRecordView.of(alertStore, "orders", "FRESHNESS");
        log.info("[端到端去重] orders FRESHNESS count={} TRIGGERED通知={}", v.count(), triggered);
        assertEquals(1, triggered);
        assertTrue(v.count() >= 2);

        // 5) 恢复：新 run 成功后下一轮 RECOVERED，不残留
        clock.setNow(480_000L);
        mockMvc.perform(post("/api/events").content(event("orders", "r2", "RUN_STARTED", 475_000, null)))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/events").content(event("orders", "r2", "SUCCEEDED", 479_000, null)))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/health/tick"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orders.status").value("HEALTHY"));
        long recovered = alertStore.notifications().stream()
                .filter(n -> n.kind().name().equals("RECOVERED")
                        && n.pipelineId().equals("orders")).count();
        log.info("[端到端恢复] orders 重新成功 -> RECOVERED 通知={}，活跃告警应为 0", recovered);
        assertEquals(1, recovered);
    }

    @Test
    void concurrentIngestionConvergesAndSlaBoundaryFires() throws Exception {
        clock.setNow(0L);
        // 并发上报 8 线程各 50 个唯一心跳 + 公共 started
        int threads = 8;
        int per = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int base = t;
            tasks.add(() -> {
                mockMvc.perform(post("/api/events")
                        .content(event("billing", "c1", "RUN_STARTED", 0, null)));
                for (int i = 1; i <= per; i++) {
                    mockMvc.perform(post("/api/events")
                            .content(event("billing", "c1", "HEARTBEAT", 1000L + base * 10_000L + i, null)));
                }
                return null;
            });
        }
        List<Future<Void>> futures = pool.invokeAll(tasks);
        for (Future<Void> f : futures) {
            f.get();
        }
        pool.shutdown();

        long accepted = runStateStore.get("c1").orElseThrow().version() - 1;
        long expected = 1L + (long) threads * per;
        log.info("[端到端并发] threads={} 唯一事件={} 实际ACCEPTED={} status={}",
                threads, expected, accepted, runStateStore.get("c1").orElseThrow().status());
        assertEquals(expected, accepted, "并发上报不得丢失或重复");

        // SLA 边界：duration=120000 未违约，120001 违约
        clock.setNow(120_000L);
        mockMvc.perform(post("/api/health/tick"))
                .andExpect(jsonPath("$.billing.status").value("HEALTHY"));
        clock.setNow(120_001L);
        mockMvc.perform(post("/api/health/tick"))
                .andExpect(jsonPath("$.billing.status").value("SLA_BREACHED"));
        log.info("[端到端SLA] 运行时长边界 120000 HEALTHY / 120001 SLA_BREACHED");

        // 调度滞后管道：配置覆盖生效，lag 边界判定
        mockMvc.perform(post("/api/events").content(event("scheduled", "s1", "RUN_STARTED", 0, null)))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/events").content(event("scheduled", "s1", "SUCCEEDED", 0, null)))
                .andExpect(status().isOk());
        clock.setNow(90_000L);
        mockMvc.perform(post("/api/health/tick"))
                .andExpect(jsonPath("$.scheduled.status").value("HEALTHY"));
        clock.setNow(90_001L);
        mockMvc.perform(post("/api/health/tick"))
                .andExpect(jsonPath("$.scheduled.status").value("SCHEDULE_LAGGING"));
        log.info("[端到端调度] scheduled interval=60s tolerance=30s：lag=30000 HEALTHY / 30001 LAGGING");
    }

    /** 小工具：以反射无关方式读取告警视图字段。 */
    private record AlertRecordView(int count) {
        static AlertRecordView of(AlertStore store, String pipeline, String type) {
            var rec = store.get(AlertStore.key(pipeline,
                    com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertType.valueOf(type)))
                    .orElseThrow();
            return new AlertRecordView(rec.count());
        }
    }
}

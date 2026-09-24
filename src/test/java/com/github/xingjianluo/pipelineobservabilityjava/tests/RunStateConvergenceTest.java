package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.EventType;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineEvent;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RunStatus;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.RunStateStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * run 级状态收敛：乱序到达、重复上报（producer 重试）、终结态迟到保护、并发上报。
 */
class RunStateConvergenceTest {

    private static final Logger log = LoggerFactory.getLogger(RunStateConvergenceTest.class);

    private final RunStateStore store = new RunStateStore();

    private PipelineEvent event(String runId, EventType type, long at) {
        return new PipelineEvent("1.0", "p1", runId, type, at, at, null, null, null, null);
    }

    @Test
    void outOfOrderEventsConverge() {
        log.info("[乱序] run=r1 事件到达顺序：SUCCEEDED(200) -> RUN_STARTED(100) -> HEARTBEAT(150)");
        var o1 = store.apply(event("r1", EventType.SUCCEEDED, 200));
        var o2 = store.apply(event("r1", EventType.RUN_STARTED, 100));
        var o3 = store.apply(event("r1", EventType.HEARTBEAT, 150));

        log.info("[乱序判定] o1={}/{}, o2={}, o3={} -> 最终 status={} startedAt={} finishedAt={} version={}",
                o1.outcome(), o1.state().status(), o2.outcome(), o3.outcome(),
                o3.state().status(), o3.state().startedAt(), o3.state().finishedAt(), o3.state().version());
        assertEquals(RunStatus.SUCCEEDED, o1.state().status());
        // 终结后到达的较早事件（新签名）必须被 LATE_IGNORED_TERMINAL，状态不回退
        assertEquals(RunStateStore.LATE_IGNORED_TERMINAL, o2.outcome());
        assertEquals(RunStateStore.LATE_IGNORED_TERMINAL, o3.outcome());
        assertEquals(RunStatus.SUCCEEDED, store.get("r1").orElseThrow().status());
        // 迟到的 RUN_STARTED 已被忽略，startedAt 不被改写，finishedAt 保持首终态时间
        assertEquals(200L, store.get("r1").orElseThrow().startedAt());
        assertEquals(200L, store.get("r1").orElseThrow().finishedAt());
    }

    @Test
    void duplicateReportsAreIdempotent() {
        var a = store.apply(event("r2", EventType.RUN_STARTED, 100));
        var b = store.apply(event("r2", EventType.RUN_STARTED, 100));
        var c = store.apply(event("r2", EventType.HEARTBEAT, 150));
        var d = store.apply(event("r2", EventType.HEARTBEAT, 150));
        log.info("[重复] RUN_STARTED x2 outcomes={},{} ; HEARTBEAT x2 outcomes={},{}; 最终 version={} heartbeatAt={}",
                a.outcome(), b.outcome(), c.outcome(), d.outcome(),
                store.get("r2").orElseThrow().version(), store.get("r2").orElseThrow().heartbeatAt());
        assertEquals(RunStateStore.ACCEPTED, a.outcome());
        assertEquals(RunStateStore.DUPLICATE_IGNORED, b.outcome());
        assertEquals(RunStateStore.ACCEPTED, c.outcome());
        assertEquals(RunStateStore.DUPLICATE_IGNORED, d.outcome());
        // 重复上报不得推动版本
        assertEquals(c.state().version(), store.get("r2").orElseThrow().version());
        assertEquals(150L, store.get("r2").orElseThrow().heartbeatAt());
    }

    @Test
    void firstFailureWinsOverLateSuccess() {
        log.info("[终结竞争] FAILED@300 先于迟到 SUCCEEDED@400 被处理");
        var failed = store.apply(event("r3", EventType.FAILED, 300));
        var lateSuccess = store.apply(event("r3", EventType.SUCCEEDED, 400));
        log.info("[终结竞争判定] failed={} lateSuccess={} -> status={}",
                failed.outcome(), lateSuccess.outcome(), store.get("r3").orElseThrow().status());
        assertEquals(RunStatus.FAILED, store.get("r3").orElseThrow().status());
        assertEquals(RunStateStore.LATE_IGNORED_TERMINAL, lateSuccess.outcome());
    }

    @Test
    void concurrentReportsConvergeConsistently() throws Exception {
        int threads = 16;
        int perThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Callable<Void>> tasks = new ArrayList<>();
        // 每个线程提交 1 次 RUN_STARTED(10) + perThread-1 次 HEARTBEAT(各不相同时间)
        for (int t = 0; t < threads; t++) {
            final int base = t;
            tasks.add(() -> {
                store.apply(event("r4", EventType.RUN_STARTED, 10));
                for (int i = 1; i < perThread; i++) {
                    long at = 20L + base * 1000L + i;
                    store.apply(event("r4", EventType.HEARTBEAT, at));
                }
                return null;
            });
        }
        List<Future<Void>> futures = pool.invokeAll(tasks);
        for (Future<Void> f : futures) {
            f.get();
        }
        pool.shutdown();

        var state = store.get("r4").orElseThrow();
        long expectedAccepted = 1L + (long) threads * (perThread - 1);
        long acceptedVersions = state.version() - 1L; // 首个事件 version=1，之后每次 ACCEPTED +1
        log.info("[并发收敛] threads={} 唯一事件数={} version-1(ACCEPTED次数)={} heartbeatAt={} status={}",
                threads, expectedAccepted, acceptedVersions, state.heartbeatAt(), state.status());
        assertEquals(expectedAccepted, acceptedVersions,
                "并发下每个唯一事件必须恰好被接受一次，不得丢失或重复计数");
        assertEquals(RunStatus.RUNNING, state.status());
        assertTrue(state.heartbeatAt() >= 20L);
    }
}

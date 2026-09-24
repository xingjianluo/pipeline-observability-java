package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertState;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthSnapshot;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthStatus;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.AlertService;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.MaintenanceService;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.AlertStore;
import com.github.xingjianluo.pipelineobservabilityjava.observability.util.MutableClock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 并发协调：多线程同时对同一管道同一类告警做 reconcile，合并计数不得丢失，
 * TRIGGERED 通知恰好一条。
 */
class AlertConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(AlertConcurrencyTest.class);

    @Test
    void concurrentReconcileMergesWithoutLostCounts() throws Exception {
        MutableClock clock = new MutableClock();
        clock.setNow(1000L);
        AlertStore store = new AlertStore();
        MaintenanceService maintenance = new MaintenanceService(clock);
        AlertService alerts = new AlertService(store, maintenance, clock);

        HealthSnapshot firing = new HealthSnapshot("p9", 1000L, HealthStatus.STALE,
                List.of("STALE: concurrent basis"), null, null, null,
                Set.of(AlertType.FRESHNESS));

        int threads = 20;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Callable<Void>> tasks = new java.util.ArrayList<>();
        for (int t = 0; t < threads; t++) {
            tasks.add(() -> {
                for (int i = 0; i < perThread; i++) {
                    alerts.reconcile(firing, 900_000L);
                }
                return null;
            });
        }
        List<Future<Void>> futures = pool.invokeAll(tasks);
        for (Future<Void> f : futures) {
            f.get();
        }
        pool.shutdown();

        var record = store.get(AlertStore.key("p9", AlertType.FRESHNESS)).orElseThrow();
        long triggered = store.notifications().stream()
                .filter(n -> n.kind().name().equals("TRIGGERED")).count();
        log.info("[并发告警] threads={} 每轮x{} 总协调={} count={} TRIGGERED={} state={}",
                threads, perThread, threads * perThread, record.count(), triggered, record.state());
        assertEquals(threads * perThread, record.count(), "并发合并计数必须等于总协调次数，不得丢失");
        assertEquals(1, triggered, "首次触发只通知一次");
        assertEquals(AlertState.ACTIVE, record.state());
    }
}

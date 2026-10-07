package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.xingjianluo.pipelineobservabilityjava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectionRecord;
import com.github.xingjianluo.pipelineobservabilityjava.service.DefaultRejectionLogService;

/**
 * 被拒留痕容量上限：明细超出容量按到达先后淘汰最旧（内存不无限增长），
 * 但按管道、按原因的累计被拒条数一条不少、始终精确；
 * 明细被淘汰后按管道/原因的明细查询与汇总查询照常可用。
 */
class RejectionLogCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(RejectionLogCapacityTest.class);
    private static final long T0 = ClockTestConfig.T0;

    private DefaultRejectionLogService newService(int capacity) {
        ObservabilityProperties props = new ObservabilityProperties();
        props.setRejectionLogCapacity(capacity);
        log.info("[CONFIG] rejectionLogCapacity={}", capacity);
        return new DefaultRejectionLogService(new MutableClock(T0), props);
    }

    @Test
    void oldestDetailsEvictedBeyondCapacityWhileCountsStayExact() {
        DefaultRejectionLogService svc = newService(50);

        // 120 条被拒：p1 交替 UNKNOWN_STATUS/MISSING_FIELD 共 80 条，p2 共 40 条
        for (int i = 1; i <= 80; i++) {
            svc.record("cap-p1", i % 2 == 0 ? RejectCode.MISSING_FIELD : RejectCode.UNKNOWN_STATUS,
                    "bad event " + i, "{\"i\":" + i + "}");
        }
        for (int i = 81; i <= 120; i++) {
            svc.record("cap-p2", RejectCode.TYPE_MISMATCH, "bad event " + i, null);
        }
        log.info("[INPUT] recorded 120 rejections (p1=80, p2=40) against capacity=50");

        // 明细只保留最新 50 条：最旧的 70 条（seq 1..70）已按到达先后淘汰
        List<RejectionRecord> all = svc.query(null, null);
        log.info("[EVICTED] retained={} oldestSeq={} newestSeq={}",
                all.size(), all.get(0).getSeq(), all.get(all.size() - 1).getSeq());
        assertEquals(50, all.size());
        assertEquals(50, svc.retainedCount());
        assertEquals(71, all.get(0).getSeq(), "oldest retained record must be seq 71 (FIFO eviction)");
        assertEquals(120, all.get(all.size() - 1).getSeq());

        // 累计计数一条不少：全局 120、p1=80（两种原因各 40）、p2=40
        assertEquals(120, svc.totalCount(null));
        assertEquals(80, svc.totalCount("cap-p1"));
        assertEquals(40, svc.totalCount("cap-p2"));
        Map<RejectCode, Long> p1 = svc.countByCode("cap-p1");
        log.info("[COUNTS] global={} p1={} p2={}", svc.totalCount(null), p1,
                svc.countByCode("cap-p2").get(RejectCode.TYPE_MISMATCH));
        assertEquals(40L, p1.get(RejectCode.UNKNOWN_STATUS));
        assertEquals(40L, p1.get(RejectCode.MISSING_FIELD));
        assertEquals(40L, svc.countByCode("cap-p2").get(RejectCode.TYPE_MISMATCH));

        // 明细查询照常可用：p1 只剩未被淘汰的 seq 71..80 共 10 条，过滤仍正确
        List<RejectionRecord> p1Missing = svc.query("cap-p1", RejectCode.MISSING_FIELD);
        log.info("[QUERY-AFTER-EVICT] p1/MISSING_FIELD retained={} firstSeq={}",
                p1Missing.size(), p1Missing.isEmpty() ? "-" : p1Missing.get(0).getSeq());
        assertEquals(5, p1Missing.size());
        assertTrue(p1Missing.stream().allMatch(r -> r.getSeq() >= 71));
        assertEquals(40, svc.query("cap-p2", null).size());

        // 继续超限：再写 10 条，容量仍 50，最旧的再退 10 条，累计到 130
        for (int i = 121; i <= 130; i++) {
            svc.record("cap-p1", RejectCode.UNKNOWN_STATUS, "more " + i, null);
        }
        List<RejectionRecord> after = svc.query(null, null);
        log.info("[EVICT-MORE] retained={} oldestSeq={} globalTotal={}",
                after.size(), after.get(0).getSeq(), svc.totalCount(null));
        assertEquals(50, after.size());
        assertEquals(81, after.get(0).getSeq());
        assertEquals(130, svc.totalCount(null));
        assertEquals(90, svc.totalCount("cap-p1"));
    }

    @Test
    void concurrentRejectionsEvictButNeverLoseCount() throws Exception {
        DefaultRejectionLogService svc = newService(100);
        int threads = 8, perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            int id = t;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int i = 0; i < perThread; i++) {
                        svc.record("cap-conc-" + (id % 3), RejectCode.UNKNOWN_STATUS,
                                "t" + id + "-" + i, null);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        long total = svc.totalCount(null);
        log.info("[CONCURRENT] threads={}x{} capacity={} retained={} globalTotal={}",
                threads, perThread, svc.capacity(), svc.retainedCount(), total);
        assertEquals(100, svc.retainedCount(), "detail log must stay within capacity");
        assertEquals(threads * perThread, total, "not a single rejection count may be lost");
        assertEquals(threads * perThread,
                svc.countByCode(null).get(RejectCode.UNKNOWN_STATUS).longValue());
        // 按管道汇总也精确：id%3 分布到 3 个管道
        long perPipelineSum = svc.totalCount("cap-conc-0") + svc.totalCount("cap-conc-1")
                + svc.totalCount("cap-conc-2");
        assertEquals(threads * perThread, perPipelineSum);
        // 并发淘汰后明细 seq 仍单调递增、无空洞之外的乱序
        List<RejectionRecord> retained = svc.query(null, null);
        for (int i = 1; i < retained.size(); i++) {
            assertTrue(retained.get(i).getSeq() > retained.get(i - 1).getSeq(),
                    "retained details must stay in arrival order");
        }
    }
}

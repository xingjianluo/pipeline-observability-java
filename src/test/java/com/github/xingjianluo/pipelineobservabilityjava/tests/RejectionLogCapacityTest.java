package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectionRecord;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.RejectionLogService;

/**
 * 拒绝留痕容量上限：明细超过 capacity 后按到达先后淘汰最旧（内存有界），
 * 但按管道、按原因的累计被拒计数一条不少；明细被淘汰后，
 * 按管道/按原因的明细查询与汇总查询照常可用。
 */
@SpringBootTest(properties = "observability.rejection-log-capacity=5")
@Import(ClockTestConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RejectionLogCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(RejectionLogCapacityTest.class);
    private static final String P = "cap-pipe";

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private RejectionLogService rejectionLog;

    private void reject(String pipeline, String status) throws Exception {
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"x\",\"pipelineName\":\""
                + pipeline + "\",\"status\":\"" + status + "\",\"eventTime\":1700000000000}";
        var r = ingestion.ingest(mapper.readTree(json));
        log.info("[INPUT] {} -> rejected code={}", json, r.getRejectCode());
    }

    @Test
    void evictionKeepsMemoryBoundedAndCountsExact() throws Exception {
        log.info("[INPUT] capacity={} rejections to send: 7x UNKNOWN_STATUS({}) + 5x UNSUPPORTED_PROTOCOL_VERSION(other-pipe)",
                rejectionLog.capacity(), P);
        assertEquals(5, rejectionLog.capacity());

        // 7 条 UNKNOWN_STATUS（管道 cap-pipe）+ 5 条 UNSUPPORTED_PROTOCOL_VERSION（管道 other-pipe）
        for (int i = 0; i < 7; i++) {
            reject(P, "BOGUS");
        }
        for (int i = 0; i < 5; i++) {
            String json = "{\"protocolVersion\":\"9.9\",\"runId\":\"y\",\"pipelineName\":\"other-pipe\","
                    + "\"status\":\"RUNNING\",\"eventTime\":1700000000000}";
            var r = ingestion.ingest(mapper.readTree(json));
            log.info("[INPUT] {} -> rejected code={}", json, r.getRejectCode());
        }

        // 明细只保留最近 5 条，最旧的 7 条按到达先后被淘汰。
        List<RejectionRecord> retained = rejectionLog.query(null, null);
        log.info("[CHECK] retained={} evicted={} retainedSeqs={}",
                retained.size(), rejectionLog.evictedCount(),
                retained.stream().map(RejectionRecord::getSeq).toList());
        assertEquals(5, retained.size(), "retained records must be bounded by capacity");
        assertEquals(5, rejectionLog.retainedCount());
        assertEquals(7, rejectionLog.evictedCount(), "oldest 7 records evicted in arrival order");
        assertEquals(List.of(8L, 9L, 10L, 11L, 12L),
                retained.stream().map(RejectionRecord::getSeq).toList(),
                "eviction must be FIFO: only the newest 5 records remain");

        // 累计计数一条不少：全局 12 = 7 UNKNOWN_STATUS + 5 UNSUPPORTED_PROTOCOL_VERSION。
        log.info("[CHECK] total={} byCode={} perPipeline({})={}",
                rejectionLog.totalCount(null), rejectionLog.countByCode(null),
                P, rejectionLog.countByCode(P));
        assertEquals(12, rejectionLog.totalCount(null), "cumulative counts survive eviction");
        assertEquals(7L, rejectionLog.countByCode(null).get(RejectCode.UNKNOWN_STATUS));
        assertEquals(5L, rejectionLog.countByCode(null).get(RejectCode.UNSUPPORTED_PROTOCOL_VERSION));
        assertEquals(7, rejectionLog.totalCount(P), "per-pipeline count exact after eviction");
        assertEquals(7L, rejectionLog.countByCode(P).get(RejectCode.UNKNOWN_STATUS));
        assertEquals(5, rejectionLog.totalCount("other-pipe"));

        // 明细查询照常可用：保留窗口内按管道/原因过滤正确
        // （7 条 cap-pipe 全部已被淘汰，5 条 other-pipe 全部在保留窗口内）。
        List<RejectionRecord> capPipeDetails = rejectionLog.query(P, RejectCode.UNKNOWN_STATUS);
        List<RejectionRecord> otherDetails =
                rejectionLog.query("other-pipe", RejectCode.UNSUPPORTED_PROTOCOL_VERSION);
        log.info("[CHECK] retained details: {}={} other-pipe={}",
                P, capPipeDetails.size(), otherDetails.size());
        assertEquals(0, capPipeDetails.size(),
                "all 7 cap-pipe records fell out of the retained window");
        assertEquals(5, otherDetails.size());
        assertTrue(otherDetails.stream().allMatch(r -> "other-pipe".equals(r.getPipelineName())
                && r.getRejectCode() == RejectCode.UNSUPPORTED_PROTOCOL_VERSION));

        // 再追加 2 条 cap-pipe 被拒：窗口滑动，cap-pipe 明细重新可查，累计计数继续精确累加。
        reject(P, "BOGUS");
        reject(P, "BOGUS");
        List<RejectionRecord> capPipeAfter = rejectionLog.query(P, RejectCode.UNKNOWN_STATUS);
        log.info("[CHECK] after 2 more: retained={} evicted={} capPipeRetained={} total({})={}",
                rejectionLog.retainedCount(), rejectionLog.evictedCount(),
                capPipeAfter.size(), P, rejectionLog.totalCount(P));
        assertEquals(5, rejectionLog.retainedCount());
        assertEquals(9, rejectionLog.evictedCount());
        assertEquals(2, capPipeAfter.size(), "newest cap-pipe records queryable again");
        assertEquals(9, rejectionLog.totalCount(P), "per-pipeline count keeps accumulating exactly");
        assertEquals(14, rejectionLog.totalCount(null));
        log.info("[ASSERT] memory bounded (retained=5), counts exact (14), queries still work");
    }
}

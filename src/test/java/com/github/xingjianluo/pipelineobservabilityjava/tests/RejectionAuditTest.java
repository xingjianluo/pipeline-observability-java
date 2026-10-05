package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.IngestResult;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectionRecord;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.RejectionLogService;

/** 被拒上报：六类原因可区分、全部留痕、可按管道/原因查询、各原因计数精确。 */
@SpringBootTest
@Import(ClockTestConfig.class)
class RejectionAuditTest {

    private static final Logger log = LoggerFactory.getLogger(RejectionAuditTest.class);
    private static final String P = "rej-audit";
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private RejectionLogService rejectionLog;

    private IngestResult send(String json) throws Exception {
        log.info("[INPUT] {}", json);
        IngestResult r = ingestion.ingest(mapper.readTree(json));
        log.info("[RESULT] accepted={} code={} msg={}", r.isAccepted(), r.getRejectCode(), r.getMessage());
        return r;
    }

    @Test
    void everyRejectCategoryIsRecordedAndQueryable() throws Exception {
        // 六类原因各制造若干次，次数故意不同，便于校验各自计数。
        send("{\"protocolVersion\":\"1.0\",\"pipelineName\":\"" + P + "\",\"status\":\"RUNNING\","
                + "\"eventTime\":1700000000000}"); // 缺 runId -> MISSING_FIELD
        send("{\"protocolVersion\":\"1.0\",\"runId\":\"ra-1\",\"pipelineName\":\"" + P + "\","
                + "\"status\":\"RUNNING\",\"eventTime\":\"soon\"}"); // eventTime 字符串 -> TYPE_MISMATCH
        send("{\"protocolVersion\":\"1.0\",\"runId\":\"ra-2\",\"pipelineName\":\"" + P + "\","
                + "\"status\":\"RUNNING\",\"eventTime\":1.5}"); // eventTime 小数 -> TYPE_MISMATCH
        send("{\"protocolVersion\":\"9.9\",\"runId\":\"ra-3\",\"pipelineName\":\"" + P + "\","
                + "\"status\":\"RUNNING\",\"eventTime\":1700000000000}"); // -> UNSUPPORTED_PROTOCOL_VERSION
        send("{\"protocolVersion\":\"1.0\",\"runId\":\"ra-4\",\"pipelineName\":\"" + P + "\","
                + "\"status\":\"EXPLODED\",\"eventTime\":1700000000000}"); // -> UNKNOWN_STATUS
        send("{\"protocolVersion\":\"1.0\",\"runId\":\"ra-5\",\"pipelineName\":\"" + P + "\","
                + "\"status\":\"RUNNING\",\"eventTime\":0}"); // -> INVALID_TIMESTAMP
        // 整条报文不是 JSON 对象 -> MALFORMED_JSON（与字段级问题区分开）
        send("[1,2,3]");

        Map<RejectCode, Long> byCode = rejectionLog.countByCode(P);
        log.info("[SUMMARY] pipeline={} byCode={} total={}", P, byCode, rejectionLog.totalCount(P));
        assertEquals(1L, byCode.get(RejectCode.MISSING_FIELD));
        assertEquals(2L, byCode.get(RejectCode.TYPE_MISMATCH));
        assertEquals(1L, byCode.get(RejectCode.UNSUPPORTED_PROTOCOL_VERSION));
        assertEquals(1L, byCode.get(RejectCode.UNKNOWN_STATUS));
        assertEquals(1L, byCode.get(RejectCode.INVALID_TIMESTAMP));
        // 非对象报文解析不出管道名，不计入该管道，但全局可见
        assertEquals(0L, byCode.get(RejectCode.MALFORMED_JSON));
        assertEquals(6L, rejectionLog.totalCount(P));
        assertTrue(rejectionLog.countByCode(null).get(RejectCode.MALFORMED_JSON) >= 1L,
                "malformed payload must be recorded globally");

        // 按管道查询：留痕包含原因、说明与原文片段
        List<RejectionRecord> mine = rejectionLog.query(P, null);
        log.info("[QUERY] pipeline={} records={}", P, mine.size());
        assertEquals(6, mine.size());
        assertTrue(mine.stream().allMatch(r -> r.getRejectCode() != null
                && r.getMessage() != null && r.getRawSnippet() != null && r.getSeq() > 0));

        // 按管道+原因过滤：只回 TYPE_MISMATCH 的两条
        List<RejectionRecord> tm = rejectionLog.query(P, RejectCode.TYPE_MISMATCH);
        log.info("[QUERY] pipeline={} code=TYPE_MISMATCH records={}", P, tm.size());
        assertEquals(2, tm.size());
        assertTrue(tm.stream().allMatch(r -> r.getRejectCode() == RejectCode.TYPE_MISMATCH
                && P.equals(r.getPipelineName())));

        // 不同原因绝不挤在一起：UNKNOWN_STATUS 的留痕里不含别的原因
        List<RejectionRecord> us = rejectionLog.query(P, RejectCode.UNKNOWN_STATUS);
        assertEquals(1, us.size());
        assertTrue(us.get(0).getMessage().contains("EXPLODED"));
    }

    @Test
    void acceptedEventsAreNotRecordedAsRejections() throws Exception {
        long before = rejectionLog.totalCount(P);
        IngestResult ok = send("{\"protocolVersion\":\"1.0\",\"runId\":\"ra-ok\","
                + "\"pipelineName\":\"" + P + "\",\"status\":\"RUNNING\",\"eventTime\":1700000000000}");
        assertTrue(ok.isAccepted());
        assertEquals(before, rejectionLog.totalCount(P), "accepted event must not be logged as rejection");
        assertFalse(rejectionLog.query(P, null).stream()
                .anyMatch(r -> r.getMessage() != null && r.getMessage().contains("ra-ok")));
    }
}

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.IngestResult;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectedEvent;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.store.RejectionLedger;

/** 六类拒绝原因必须可区分、可留痕、可按管道/原因查询，且各类计数精确。 */
@SpringBootTest
@Import(ClockTestConfig.class)
class RejectionLedgerTest {

    private static final Logger log = LoggerFactory.getLogger(RejectionLedgerTest.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private RejectionLedger ledger;

    private IngestResult send(String json) {
        try {
            log.info("[INPUT] {}", json);
            JsonNode node = mapper.readTree(json);
            IngestResult r = ingestion.ingest(node);
            log.info("[RESULT] accepted={} code={} msg={}", r.isAccepted(), r.getRejectCode(), r.getMessage());
            return r;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String obj(String pipeline, String runId, String tail) {
        return "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\""
                + pipeline + "\"," + tail + "}";
    }

    @Test
    void sixRejectCategoriesAreDistinctAndRecorded() {
        String p = "rej-cat";

        // 1) 整条报文不合法（请求体是数组而非对象）=> MALFORMED_PAYLOAD
        IngestResult malformed = ingestion.ingest(mapper.createArrayNode());
        log.info("[RESULT] non-object code={}", malformed.getRejectCode());

        // 2) 必填缺失（缺 runId）=> MISSING_FIELD
        IngestResult missing = send("{\"protocolVersion\":\"1.0\",\"pipelineName\":\"" + p
                + "\",\"status\":\"RUNNING\",\"eventTime\":2}");
        // 3) 字段类型不对（protocolVersion 传数字）=> TYPE_MISMATCH
        IngestResult type1 = send("{\"protocolVersion\":10,\"runId\":\"r2\",\"pipelineName\":\""
                + p + "\",\"status\":\"RUNNING\",\"eventTime\":2}");
        // 3b) eventTime 传小数 => TYPE_MISMATCH
        IngestResult type2 = send("{\"protocolVersion\":\"1.0\",\"runId\":\"r6\",\"pipelineName\":\""
                + p + "\",\"status\":\"RUNNING\",\"eventTime\":1.5}");
        // 4) 协议版本不支持 => UNSUPPORTED_PROTOCOL_VERSION
        IngestResult proto = send("{\"protocolVersion\":\"9.9\",\"runId\":\"r3\",\"pipelineName\":\""
                + p + "\",\"status\":\"RUNNING\",\"eventTime\":2}");
        // 5) 未知状态 => UNKNOWN_STATUS
        IngestResult status = send(obj(p, "r4", "\"status\":\"EXPLODED\",\"eventTime\":2"));
        // 6) 时间非法（非正数）=> INVALID_TIMESTAMP
        IngestResult ts = send(obj(p, "r5", "\"status\":\"RUNNING\",\"eventTime\":-5"));

        assertEquals(RejectCode.MALFORMED_PAYLOAD, malformed.getRejectCode());
        assertFalse(malformed.isAccepted());
        assertEquals(RejectCode.MISSING_FIELD, missing.getRejectCode());
        assertEquals(RejectCode.TYPE_MISMATCH, type1.getRejectCode());
        assertEquals(RejectCode.TYPE_MISMATCH, type2.getRejectCode());
        assertEquals(RejectCode.UNSUPPORTED_PROTOCOL_VERSION, proto.getRejectCode());
        assertEquals(RejectCode.UNKNOWN_STATUS, status.getRejectCode());
        assertEquals(RejectCode.INVALID_TIMESTAMP, ts.getRejectCode());

        Map<RejectCode, Long> stats = ledger.countByReason(p);
        log.info("[STATS] pipeline={} byReason={}", p, stats);
        assertEquals(1L, stats.get(RejectCode.MISSING_FIELD));
        assertEquals(2L, stats.get(RejectCode.TYPE_MISMATCH),
                "numeric protocolVersion + fractional eventTime");
        assertEquals(1L, stats.get(RejectCode.UNSUPPORTED_PROTOCOL_VERSION));
        assertEquals(1L, stats.get(RejectCode.UNKNOWN_STATUS));
        assertEquals(1L, stats.get(RejectCode.INVALID_TIMESTAMP));
        // 非对象报文无法识别管道，不进该管道桶
        assertEquals(0L, stats.get(RejectCode.MALFORMED_PAYLOAD));
        assertEquals(6L, ledger.totalCount(p));

        // 全量计数里六类齐全，MALFORMED_PAYLOAD 至少 1
        Map<RejectCode, Long> all = ledger.countByReason(null);
        log.info("[STATS] all byReason={}", all);
        assertTrue(all.get(RejectCode.MALFORMED_PAYLOAD) >= 1L);

        // 按原因过滤留痕，明细可自查（含原始报文与管道）
        List<RejectedEvent> unknown = ledger.query(p, RejectCode.UNKNOWN_STATUS);
        assertEquals(1, unknown.size());
        RejectedEvent rec = unknown.get(0);
        assertEquals(p, rec.getPipelineName());
        assertEquals(RejectCode.UNKNOWN_STATUS, rec.getRejectCode());
        assertTrue(rec.getReason().contains("EXPLODED"));
        assertTrue(rec.getRawPayload().contains("EXPLODED"));
        log.info("[TRACE] pipeline={} code={} reason={} payload={}",
                rec.getPipelineName(), rec.getRejectCode(), rec.getReason(), rec.getRawPayload());

        // 管道隔离
        assertEquals(0, ledger.query("rej-other", null).size());
    }

    @Test
    void queryByPipelineAndReasonAndTotals() {
        String a = "rej-a";
        String b = "rej-b";
        send(obj(a, "a1", "\"status\":\"RUNNING\""));                       // 缺 eventTime
        send(obj(a, "a2", "\"status\":\"RUNNING\",\"eventTime\":0"));       // 时间非法
        send(obj(b, "b1", "\"status\":\"RUNNING\""));                       // 缺 eventTime

        assertEquals(2, ledger.query(a, null).size());
        assertEquals(1, ledger.query(a, RejectCode.INVALID_TIMESTAMP).size());
        assertEquals(1, ledger.query(b, RejectCode.MISSING_FIELD).size());
        assertEquals(2L, ledger.totalCount(a));
        assertEquals(1L, ledger.totalCount(b));
        log.info("[TOTALS] a={} b={} a-byReason={}",
                ledger.totalCount(a), ledger.totalCount(b), ledger.countByReason(a));
    }
}

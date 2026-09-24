package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;

/** 字段缺失 / 类型不匹配 / 协议版本不兼容 / 未知状态：必须可区分地拒绝，不静默丢弃。 */
@SpringBootTest
@Import(ClockTestConfig.class)
class EventIngestionValidationTest {

    private static final Logger log = LoggerFactory.getLogger(EventIngestionValidationTest.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;

    private IngestResult send(String json) throws Exception {
        log.info("[INPUT] {}", json);
        JsonNode node = mapper.readTree(json);
        IngestResult r = ingestion.ingest(node);
        log.info("[RESULT] accepted={} code={} msg={}", r.isAccepted(), r.getRejectCode(), r.getMessage());
        return r;
    }

    private String base(String runId) {
        return "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\"p\","
                + "\"status\":\"RUNNING\",\"eventTime\":1700000000000}";
    }

    @Test
    void rejectsMissingField() throws Exception {
        String json = "{\"protocolVersion\":\"1.0\",\"pipelineName\":\"p\",\"status\":\"RUNNING\"}";
        IngestResult r = send(json);
        assertFalse(r.isAccepted());
        assertEquals(RejectCode.MISSING_FIELD, r.getRejectCode());
        assertTrue(r.getMessage().contains("runId"));
    }

    @Test
    void rejectsTypeMismatch() throws Exception {
        String json = "{\"protocolVersion\":10,\"runId\":\"r2\",\"pipelineName\":\"p\","
                + "\"status\":\"RUNNING\",\"eventTime\":1700000000000}";
        IngestResult r = send(json);
        assertFalse(r.isAccepted());
        assertEquals(RejectCode.TYPE_MISMATCH, r.getRejectCode());
    }

    @Test
    void rejectsUnsupportedProtocol() throws Exception {
        String json = base("r3").replace("1.0", "9.9");
        IngestResult r = send(json);
        assertFalse(r.isAccepted());
        assertEquals(RejectCode.UNSUPPORTED_PROTOCOL_VERSION, r.getRejectCode());
    }

    @Test
    void rejectsUnknownStatus() throws Exception {
        String json = base("r4").replace("RUNNING", "EXPLODED");
        IngestResult r = send(json);
        assertFalse(r.isAccepted());
        assertEquals(RejectCode.UNKNOWN_STATUS, r.getRejectCode());
    }

    @Test
    void rejectsInvalidTimestamp() throws Exception {
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"r5\",\"pipelineName\":\"p\","
                + "\"status\":\"RUNNING\",\"eventTime\":-1}";
        IngestResult r = send(json);
        assertFalse(r.isAccepted());
        assertEquals(RejectCode.INVALID_TIMESTAMP, r.getRejectCode());
    }

    @Test
    void acceptsValidEvent() throws Exception {
        IngestResult r = send(base("r-valid"));
        assertTrue(r.isAccepted());
        assertFalse(r.isDuplicate());
    }
}

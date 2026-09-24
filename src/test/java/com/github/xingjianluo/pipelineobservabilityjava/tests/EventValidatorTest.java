package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.EventType;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineEvent;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RejectionReason;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.EventRejectedException;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.EventValidator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事件校验：字段缺失 / 类型不匹配 / 协议版本不兼容 / 非法取值 / 未知事件类型 必须可区分，不得静默丢弃。
 */
class EventValidatorTest {

    private static final Logger log = LoggerFactory.getLogger(EventValidatorTest.class);

    private final EventValidator validator = new EventValidator();

    @Test
    void validEventParsed() {
        String json = """
                {"protocolVersion":"1.0","pipelineId":"p1","runId":"r1",
                 "eventType":"RUN_STARTED","occurredAt":1000,"attempt":1}
                """;
        PipelineEvent event = validator.validate(json, 2000L);
        log.info("[有效事件] input={} -> type={} occurredAt={} receivedAt={}",
                json.replaceAll("\\s+", ""), event.eventType(), event.occurredAt(), event.receivedAt());
        assertEquals(EventType.RUN_STARTED, event.eventType());
        assertEquals(1000L, event.occurredAt());
        assertEquals(2000L, event.receivedAt());
    }

    @Test
    void rejectionReasonsAreDistinguishable() {
        assertReason("not-json", RejectionReason.MALFORMED_JSON);
        assertReason("[]", RejectionReason.MALFORMED_JSON);
        // 空对象：最先缺失的是 protocolVersion -> MISSING_FIELD（而非静默丢弃）
        assertReason("{}", RejectionReason.MISSING_FIELD);
        // 必填业务字段缺失
        assertReason("""
                {"protocolVersion":"1.0"}
                """, RejectionReason.MISSING_FIELD);
        assertReason("""
                {"protocolVersion":"2.0","pipelineId":"p","runId":"r",
                 "eventType":"HEARTBEAT","occurredAt":1}
                """, RejectionReason.UNSUPPORTED_PROTOCOL_VERSION);
        assertReason("""
                {"protocolVersion":1,"pipelineId":"p","runId":"r",
                 "eventType":"HEARTBEAT","occurredAt":1}
                """, RejectionReason.TYPE_MISMATCH);
        assertReason("""
                {"protocolVersion":"1.0","pipelineId":"p","runId":"r",
                 "eventType":"HEARTBEAT"}
                """, RejectionReason.MISSING_FIELD);
        assertReason("""
                {"protocolVersion":"1.0","pipelineId":123,"runId":"r",
                 "eventType":"HEARTBEAT","occurredAt":1}
                """, RejectionReason.TYPE_MISMATCH);
        assertReason("""
                {"protocolVersion":"1.0","pipelineId":"p","runId":"r",
                 "eventType":"HEARTBEAT","occurredAt":"1000"}
                """, RejectionReason.TYPE_MISMATCH);
        assertReason("""
                {"protocolVersion":"1.0","pipelineId":"p","runId":"r",
                 "eventType":"HEARTBEAT","occurredAt":1000.5}
                """, RejectionReason.TYPE_MISMATCH);
        assertReason("""
                {"protocolVersion":"1.0","pipelineId":"p","runId":"r",
                 "eventType":"HEARTBEAT","occurredAt":-5}
                """, RejectionReason.INVALID_FIELD_VALUE);
        assertReason("""
                {"protocolVersion":"1.0","pipelineId":"p","runId":"r",
                 "eventType":"HEARTBEAT","occurredAt":1,"attempt":0}
                """, RejectionReason.INVALID_FIELD_VALUE);
        assertReason("""
                {"protocolVersion":"1.0","pipelineId":"p","runId":"r",
                 "eventType":"EXPLODED","occurredAt":1}
                """, RejectionReason.UNKNOWN_EVENT_TYPE);
        assertReason("""
                {"protocolVersion":"1.0","pipelineId":"  ","runId":"r",
                 "eventType":"HEARTBEAT","occurredAt":1}
                """, RejectionReason.INVALID_FIELD_VALUE);
    }

    private void assertReason(String json, RejectionReason expected) {
        EventRejectedException ex = assertThrows(EventRejectedException.class,
                () -> validator.validate(json, 1L));
        log.info("[拒绝用例] input={} -> reason={} detail={}",
                json.replaceAll("\\s+", ""), ex.reason(), ex.getMessage());
        assertEquals(expected, ex.reason(),
                "payload [" + json + "] 应被拒绝为 " + expected + "，实际 " + ex.reason());
    }
}

package com.github.xingjianluo.pipelineobservabilityjava.observability.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.EventType;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineEvent;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RejectionReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 将原始 JSON 上报校验并转换为 {@link PipelineEvent}。
 *
 * <p>校验顺序固定，保证拒绝原因可区分：</p>
 * <ol>
 *   <li>{@code MALFORMED_JSON}：非 JSON 对象、无法解析；</li>
 *   <li>{@code UNSUPPORTED_PROTOCOL_VERSION}：protocolVersion 缺失先归 MISSING_FIELD，
 *       存在但非 "1.0" 归本类；</li>
 *   <li>{@code MISSING_FIELD}：pipelineId/runId/eventType/occurredAt 缺字段或为 null；</li>
 *   <li>{@code TYPE_MISMATCH}：字段 JSON 类型不符（如 occurredAt 传字符串/布尔）；</li>
 *   <li>{@code INVALID_FIELD_VALUE}：空字符串、负数时间、attempt<1、时间为浮点等；</li>
 *   <li>{@code UNKNOWN_EVENT_TYPE}：eventType 不在枚举集合内。</li>
 * </ol>
 */
@Component
public class EventValidator {

    private static final Logger log = LoggerFactory.getLogger(EventValidator.class);

    public static final String SUPPORTED_VERSION = "1.0";

    private final ObjectMapper objectMapper = new ObjectMapper();

    public PipelineEvent validate(String rawJson, long receivedAt) {
        if (rawJson == null || rawJson.isBlank()) {
            throw rejected(RejectionReason.MALFORMED_JSON, "request body is empty");
        }

        JsonNode root;
        try {
            root = objectMapper.readTree(rawJson);
        } catch (JsonProcessingException ex) {
            throw rejected(RejectionReason.MALFORMED_JSON, "payload is not valid JSON: " + ex.getOriginalMessage());
        }
        if (root == null || !root.isObject()) {
            throw rejected(RejectionReason.MALFORMED_JSON, "payload root must be a JSON object");
        }

        // 协议版本：缺字段 -> MISSING_FIELD；非字符串 -> TYPE_MISMATCH；不支持 -> UNSUPPORTED_PROTOCOL_VERSION
        if (!root.has("protocolVersion") || root.get("protocolVersion").isNull()) {
            throw rejected(RejectionReason.MISSING_FIELD, "protocolVersion is required");
        }
        JsonNode versionNode = root.get("protocolVersion");
        if (!versionNode.isTextual()) {
            throw rejected(RejectionReason.TYPE_MISMATCH, "protocolVersion must be a string");
        }
        if (!SUPPORTED_VERSION.equals(versionNode.asText())) {
            throw rejected(RejectionReason.UNSUPPORTED_PROTOCOL_VERSION,
                    "protocolVersion '" + versionNode.asText() + "' is not supported, expected '"
                            + SUPPORTED_VERSION + "'");
        }

        String pipelineId = requireText(root, "pipelineId");
        String runId = requireText(root, "runId");
        long occurredAt = requireLong(root, "occurredAt");

        JsonNode typeNode = root.get("eventType");
        if (!typeNode.isTextual()) {
            throw rejected(RejectionReason.TYPE_MISMATCH, "eventType must be a string");
        }
        EventType eventType;
        try {
            eventType = EventType.valueOf(typeNode.asText());
        } catch (IllegalArgumentException ex) {
            throw rejected(RejectionReason.UNKNOWN_EVENT_TYPE,
                    "eventType '" + typeNode.asText() + "' is not one of "
                            + java.util.Arrays.toString(EventType.values()));
        }

        Long scheduledStartAt = optionalLong(root, "scheduledStartAt");
        Long slaDeadlineAt = optionalLong(root, "slaDeadlineAt");
        Integer attempt = optionalInt(root, "attempt");
        String message = optionalText(root, "message");

        PipelineEvent event = new PipelineEvent(versionNode.asText(), pipelineId, runId, eventType,
                occurredAt, receivedAt, scheduledStartAt, slaDeadlineAt, attempt, message);
        log.info("EventValidator.ok | pipelineId={} runId={} type={} occurredAt={} receivedAt={}",
                pipelineId, runId, eventType, occurredAt, receivedAt);
        return event;
    }

    private String requireText(JsonNode root, String field) {
        if (!root.has(field) || root.get(field).isNull()) {
            throw rejected(RejectionReason.MISSING_FIELD, field + " is required");
        }
        JsonNode node = root.get(field);
        if (!node.isTextual()) {
            throw rejected(RejectionReason.TYPE_MISMATCH, field + " must be a string");
        }
        String value = node.asText();
        if (value.isBlank()) {
            throw rejected(RejectionReason.INVALID_FIELD_VALUE, field + " must not be blank");
        }
        return value;
    }

    private long requireLong(JsonNode root, String field) {
        if (!root.has(field) || root.get(field).isNull()) {
            throw rejected(RejectionReason.MISSING_FIELD, field + " is required");
        }
        return asLong(root.get(field), field);
    }

    private Long optionalLong(JsonNode root, String field) {
        if (!root.has(field) || root.get(field).isNull()) {
            return null;
        }
        return asLong(root.get(field), field);
    }

    private Integer optionalInt(JsonNode root, String field) {
        if (!root.has(field) || root.get(field).isNull()) {
            return null;
        }
        JsonNode node = root.get(field);
        if (node.isTextual() || node.isBoolean() || node.isObject() || node.isArray()) {
            throw rejected(RejectionReason.TYPE_MISMATCH, field + " must be an integer");
        }
        if (!node.isIntegralNumber()) {
            throw rejected(RejectionReason.TYPE_MISMATCH, field + " must be an integer (fractional value given)");
        }
        long raw = node.asLong();
        if (raw < 1) {
            throw rejected(RejectionReason.INVALID_FIELD_VALUE, field + " must be >= 1");
        }
        return (int) raw;
    }

    private String optionalText(JsonNode root, String field) {
        if (!root.has(field) || root.get(field).isNull()) {
            return null;
        }
        JsonNode node = root.get(field);
        if (!node.isTextual()) {
            throw rejected(RejectionReason.TYPE_MISMATCH, field + " must be a string");
        }
        return node.asText();
    }

    private long asLong(JsonNode node, String field) {
        if (node.isTextual() || node.isBoolean() || node.isObject() || node.isArray()) {
            throw rejected(RejectionReason.TYPE_MISMATCH, field + " must be an integer (epoch millis)");
        }
        if (!node.isIntegralNumber()) {
            throw rejected(RejectionReason.TYPE_MISMATCH, field + " must be an integer (fractional value given)");
        }
        long value = node.asLong();
        if (value < 0) {
            throw rejected(RejectionReason.INVALID_FIELD_VALUE, field + " must be >= 0 epoch millis");
        }
        return value;
    }

    private EventRejectedException rejected(RejectionReason reason, String detail) {
        log.warn("EventValidator.reject | reason={} detail={}", reason, detail);
        return new EventRejectedException(reason, detail);
    }
}

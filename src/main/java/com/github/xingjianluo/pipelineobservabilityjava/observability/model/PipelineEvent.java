package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 规范化后的管道运行事件。原始上报为 JSON，经 EventValidator 校验转换得到。
 *
 * @param protocolVersion  协议版本，仅支持 "1.0"
 * @param pipelineId       管道标识（必填）
 * @param runId            运行标识（必填）
 * @param eventType        事件类型（必填）
 * @param occurredAt       事件发生时间（epoch ms，必填）
 * @param receivedAt       服务接收时间（epoch ms，服务端注入）
 * @param scheduledStartAt 调度起始时间（epoch ms，可空）
 * @param slaDeadlineAt    SLA 截止时间（epoch ms，可空）
 * @param attempt          第几次尝试（可空，>=1）
 * @param message          附加信息（可空）
 */
public record PipelineEvent(
        String protocolVersion,
        String pipelineId,
        String runId,
        EventType eventType,
        long occurredAt,
        long receivedAt,
        Long scheduledStartAt,
        Long slaDeadlineAt,
        Integer attempt,
        String message
) {
}

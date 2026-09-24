package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 去重后的告警记录（不可变快照）。
 * key = pipelineId + ":" + alertType；同一 key 的重复告警合并计数，不产生重复通知。
 */
public record AlertRecord(
        String key,
        String pipelineId,
        AlertType alertType,
        AlertState state,
        AlertLevel level,
        int count,
        long firstTriggeredAt,
        long lastTriggeredAt,
        Long escalatedAt,
        Long recoveredAt,
        boolean suppressed,
        String lastReason,
        long version
) {
}

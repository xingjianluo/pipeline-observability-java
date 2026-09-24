package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 告警通知流水（触发/升级/恢复各一条；重复合并不产生新通知）。
 */
public record AlertNotification(
        long at,
        String alertKey,
        String pipelineId,
        AlertType alertType,
        AlertLevel level,
        NotificationKind kind,
        int mergedCount,
        String reason
) {
}

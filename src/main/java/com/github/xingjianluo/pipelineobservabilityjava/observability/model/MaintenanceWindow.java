package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 维护窗口。pipelineId 为 null 表示全局窗口。窗口内告警抑制（SUPPRESSED）但仍可追溯。
 */
public record MaintenanceWindow(
        String id,
        String pipelineId,
        long startAt,
        long endAt,
        String reason
) {
}

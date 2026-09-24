package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

import java.util.List;
import java.util.Set;

/**
 * 健康判定结论快照。reasons 以可读字符串记录判定依据（边界值、阈值、所用时钟等），保证可解释。
 */
public record HealthSnapshot(
        String pipelineId,
        long evaluatedAt,
        HealthStatus status,
        List<String> reasons,
        Long freshnessAgeMs,
        Long scheduleLagMs,
        Long currentRunDurationMs,
        Set<AlertType> firingAlertTypes
) {
}

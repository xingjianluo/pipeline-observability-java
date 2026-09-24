package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * ACTIVE：触发中（已通知）；ESCALATED：已升级；SUPPRESSED：维护窗口内被抑制（仍可追溯）；
 * RECOVERED：已恢复（仅留存历史）。
 */
public enum AlertState {
    ACTIVE,
    ESCALATED,
    SUPPRESSED,
    RECOVERED
}

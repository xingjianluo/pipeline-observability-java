package com.github.xingjianluo.pipelineobservabilityjava.model;

/**
 * FIRING     告警触发中（可能被维护窗口抑制通知）；
 * RECOVERED  已恢复，不再发送通知但记录可追溯；
 * SUPPRESSED 维护窗口内抑制（状态仍可查）。
 */
public enum AlertState {
    FIRING,
    SUPPRESSED,
    RECOVERED
}

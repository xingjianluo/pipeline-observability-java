package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 上报事件类型（事件协议 v1）。
 */
public enum EventType {
    RUN_STARTED,
    HEARTBEAT,
    SUCCEEDED,
    FAILED,
    CANCELLED
}

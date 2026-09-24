package com.github.xingjianluo.pipelineobservabilityjava.observability.service;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RejectionReason;

/**
 * 事件校验失败时抛出，携带可区分的拒绝原因与明细。
 */
public class EventRejectedException extends RuntimeException {

    private final RejectionReason reason;

    public EventRejectedException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason reason() {
        return reason;
    }
}

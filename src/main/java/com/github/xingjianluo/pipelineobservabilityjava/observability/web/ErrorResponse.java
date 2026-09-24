package com.github.xingjianluo.pipelineobservabilityjava.observability.web;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RejectionReason;

/**
 * 错误响应体（拒绝原因可区分，便于调用方处理）。
 */
public record ErrorResponse(String error, RejectionReason reason, String detail) {
}

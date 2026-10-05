package com.github.xingjianluo.pipelineobservabilityjava.web;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.github.xingjianluo.pipelineobservabilityjava.model.IngestResult;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.service.RejectionLogService;

/** 统一把接入层错误转成带 rejectCode 的响应，任何坏事件都不静默。 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    private final RejectionLogService rejectionLog;

    public ApiExceptionHandler(RejectionLogService rejectionLog) {
        this.rejectionLog = rejectionLog;
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<IngestResult> unreadable(HttpMessageNotReadableException e) {
        log.warn("malformed json body: {}", e.getMessage());
        String msg = "malformed or non-JSON request body: " + e.getMostSpecificCause().getMessage();
        // 整条报文格式不合法同样留痕（此时无法解析出管道名）。
        rejectionLog.record(null, RejectCode.MALFORMED_JSON, msg, null);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(IngestResult.rejected(RejectCode.MALFORMED_JSON, msg));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> illegalArg(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}

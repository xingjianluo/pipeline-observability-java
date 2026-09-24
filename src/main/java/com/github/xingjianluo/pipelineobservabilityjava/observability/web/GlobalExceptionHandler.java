package com.github.xingjianluo.pipelineobservabilityjava.observability.web;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RejectionReason;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.EventRejectedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(EventRejectedException.class)
    public ResponseEntity<ErrorResponse> handleRejected(EventRejectedException ex) {
        HttpStatus status = ex.reason() == RejectionReason.MALFORMED_JSON
                ? HttpStatus.BAD_REQUEST : HttpStatus.UNPROCESSABLE_ENTITY;
        return ResponseEntity.status(status)
                .body(new ErrorResponse("EVENT_REJECTED", ex.reason(), ex.getMessage()));
    }
}

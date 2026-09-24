package com.github.xingjianluo.pipelineobservabilityjava.observability.web;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.IngestResult;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RejectionRecord;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.IngestService;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 运行事件接入：
 * POST /api/events 上报原始 JSON；成功返回接入结论（ACCEPTED/DUPLICATE_IGNORED/LATE_IGNORED_TERMINAL）；
 * 校验失败由 GlobalExceptionHandler 返回 400/422 及可区分原因；
 * GET /api/events/rejections 查看拒绝留痕（不静默丢弃）。
 */
@RestController
@RequestMapping("/api/events")
public class IngestController {

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping
    public ResponseEntity<IngestResult> report(@RequestBody(required = false) String rawJson) {
        return ResponseEntity.ok(ingestService.ingest(rawJson));
    }

    @GetMapping("/rejections")
    public List<RejectionRecord> rejections(@RequestParam(defaultValue = "50") int limit) {
        return ingestService.recentRejections(limit);
    }
}

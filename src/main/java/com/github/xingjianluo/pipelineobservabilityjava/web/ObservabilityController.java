package com.github.xingjianluo.pipelineobservabilityjava.web;

import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;
import com.github.xingjianluo.pipelineobservabilityjava.model.IngestResult;
import com.github.xingjianluo.pipelineobservabilityjava.model.MaintenanceWindow;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineRunState;
import com.github.xingjianluo.pipelineobservabilityjava.model.ReconciliationReport;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectedEvent;
import com.github.xingjianluo.pipelineobservabilityjava.service.AlertService;
import com.github.xingjianluo.pipelineobservabilityjava.service.DefaultEventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.HealthEvaluationService;
import com.github.xingjianluo.pipelineobservabilityjava.service.MaintenanceService;
import com.github.xingjianluo.pipelineobservabilityjava.service.ReconciliationService;
import com.github.xingjianluo.pipelineobservabilityjava.store.RejectionLedger;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/** 内置接口：事件上报、健康查询、告警查询、拒绝留痕、对账与维护窗口管理。 */
@RestController
@RequestMapping("/api/observability")
public class ObservabilityController {

    private final EventIngestionService ingestion;
    private final HealthEvaluationService health;
    private final AlertService alerts;
    private final MaintenanceService maintenance;
    private final RunStateStore store;
    private final SystemClock clock;
    private final RejectionLedger rejectionLedger;
    private final ReconciliationService reconciliation;

    public ObservabilityController(EventIngestionService ingestion,
                                   HealthEvaluationService health,
                                   AlertService alerts,
                                   MaintenanceService maintenance,
                                   RunStateStore store,
                                   SystemClock clock,
                                   RejectionLedger rejectionLedger,
                                   ReconciliationService reconciliation) {
        this.ingestion = ingestion;
        this.health = health;
        this.alerts = alerts;
        this.maintenance = maintenance;
        this.store = store;
        this.clock = clock;
        this.rejectionLedger = rejectionLedger;
        this.reconciliation = reconciliation;
    }

    @PostMapping("/events")
    public IngestResult reportEvent(@RequestBody JsonNode raw) {
        return ingestion.ingest(raw);
    }

    /** 供异常处理器复用：畸形 JSON 也要按 MALFORMED_PAYLOAD 留痕、计数。 */
    IngestResult reportMalformed(String rawPayload) {
        return ((DefaultEventIngestionService) ingestion)
                .ingest(null, rawPayload, RejectCode.MALFORMED_PAYLOAD);
    }

    @GetMapping("/health/{pipeline}")
    public HealthAssessment health(@PathVariable String pipeline) {
        return health.assess(pipeline, clock.nowMillis());
    }

    @GetMapping("/health")
    public List<HealthAssessment> healthAll() {
        return health.assessAll(clock.nowMillis());
    }

    /** 单管道运行质量与上报告警对账（健康结论、告警状态、各项计数同一口径）。 */
    @GetMapping("/reconciliation/{pipeline}")
    public ReconciliationReport reconciliation(@PathVariable String pipeline) {
        return reconciliation.report(pipeline, clock.nowMillis());
    }

    @GetMapping("/reconciliation")
    public List<ReconciliationReport> reconciliationAll() {
        return reconciliation.reportAll(clock.nowMillis());
    }

    /** 被拒上报留痕明细：可按 pipeline 与 reason(RejectCode) 过滤。 */
    @GetMapping("/rejections")
    public List<RejectedEvent> rejections(
            @RequestParam(required = false) String pipeline,
            @RequestParam(required = false) RejectCode reason) {
        return rejectionLedger.query(pipeline, reason);
    }

    /** 每种拒绝原因的累计计数；pipeline 省略为全量。 */
    @GetMapping("/rejections/stats")
    public Map<String, Object> rejectionStats(@RequestParam(required = false) String pipeline) {
        return Map.of(
                "pipeline", pipeline == null ? "*" : pipeline,
                "total", rejectionLedger.totalCount(pipeline),
                "byReason", rejectionLedger.countByReason(pipeline)
        );
    }

    @GetMapping("/alerts")
    public List<Alert> alerts(@RequestParam(defaultValue = "false") boolean includeRecovered) {
        return alerts.list(includeRecovered);
    }

    @PostMapping("/maintenance")
    public MaintenanceWindow addMaintenance(@RequestBody JsonNode body) {
        String pipeline = body.hasNonNull("pipelineName") ? body.get("pipelineName").asText() : null;
        long start = body.path("startAt").asLong(clock.nowMillis());
        long end = body.path("endAt").asLong(clock.nowMillis());
        String note = body.path("note").asText(null);
        return maintenance.add(pipeline, start, end, note);
    }

    @GetMapping("/maintenance")
    public List<MaintenanceWindow> listMaintenance() {
        return maintenance.list();
    }

    @PostMapping("/alerts/{pipeline}/{type}/recover")
    public Alert recover(@PathVariable String pipeline, @PathVariable String type) {
        return alerts.recover(pipeline, AlertType.valueOf(type), clock.nowMillis());
    }

    @GetMapping("/runs/{runId}")
    public PipelineRunState run(@PathVariable String runId) {
        return store.find(runId).orElse(null);
    }

    @GetMapping("/snapshot")
    public Map<String, Object> snapshot(
            @RequestParam(defaultValue = "false") boolean includeRecovered) {
        long now = clock.nowMillis();
        return Map.of(
                "now", now,
                "health", health.assessAll(now),
                "activeAlerts", alerts.list(includeRecovered),
                "maintenance", maintenance.list()
        );
    }
}

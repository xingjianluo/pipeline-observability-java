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
import com.github.xingjianluo.pipelineobservabilityjava.service.AlertService;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.HealthEvaluationService;
import com.github.xingjianluo.pipelineobservabilityjava.service.MaintenanceService;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/** 内置接口：事件上报、健康查询、告警查询与维护窗口管理。 */
@RestController
@RequestMapping("/api/observability")
public class ObservabilityController {

    private final EventIngestionService ingestion;
    private final HealthEvaluationService health;
    private final AlertService alerts;
    private final MaintenanceService maintenance;
    private final RunStateStore store;
    private final SystemClock clock;

    public ObservabilityController(EventIngestionService ingestion,
                                   HealthEvaluationService health,
                                   AlertService alerts,
                                   MaintenanceService maintenance,
                                   RunStateStore store,
                                   SystemClock clock) {
        this.ingestion = ingestion;
        this.health = health;
        this.alerts = alerts;
        this.maintenance = maintenance;
        this.store = store;
        this.clock = clock;
    }

    @PostMapping("/events")
    public IngestResult reportEvent(@RequestBody JsonNode raw) {
        return ingestion.ingest(raw);
    }

    @GetMapping("/health/{pipeline}")
    public HealthAssessment health(@PathVariable String pipeline) {
        return health.assess(pipeline, clock.nowMillis());
    }

    @GetMapping("/health")
    public List<HealthAssessment> healthAll() {
        return health.assessAll(clock.nowMillis());
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

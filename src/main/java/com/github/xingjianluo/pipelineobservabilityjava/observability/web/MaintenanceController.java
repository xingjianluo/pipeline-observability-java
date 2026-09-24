package com.github.xingjianluo.pipelineobservabilityjava.observability.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.MaintenanceWindow;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.MaintenanceService;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 维护窗口管理：窗口内告警抑制但仍留痕。 */
@RestController
@RequestMapping("/api/maintenance/windows")
public class MaintenanceController {

    private final MaintenanceService maintenanceService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MaintenanceController(MaintenanceService maintenanceService) {
        this.maintenanceService = maintenanceService;
    }

    @GetMapping
    public Map<String, List<MaintenanceWindow>> list() {
        return Map.of("windows", maintenanceService.listWindows());
    }

    @PostMapping
    public MaintenanceWindow open(@RequestBody String rawJson) {
        try {
            JsonNode node = objectMapper.readTree(rawJson);
            String pipelineId = node.hasNonNull("pipelineId") ? node.get("pipelineId").asText() : null;
            long startAt = node.path("startAt").asLong(0L);
            long endAt = node.path("endAt").asLong(0L);
            String reason = node.path("reason").asText("manual maintenance");
            return maintenanceService.openWindow(pipelineId, startAt, endAt, reason);
        } catch (Exception ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "invalid maintenance window payload: " + ex.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    public Map<String, Boolean> close(@PathVariable String id) {
        boolean removed = maintenanceService.closeWindow(id);
        if (!removed) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "maintenance window not found: " + id);
        }
        return Map.of("closed", true);
    }
}

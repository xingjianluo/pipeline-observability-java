package com.github.xingjianluo.pipelineobservabilityjava.observability.web;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertNotification;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertRecord;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.AlertService;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 告警与通知流水查询（默认不返回已恢复告警，可显式包含）。 */
@RestController
@RequestMapping("/api/alerts")
public class AlertController {

    private final AlertService alertService;

    public AlertController(AlertService alertService) {
        this.alertService = alertService;
    }

    @GetMapping
    public Map<String, List<AlertRecord>> alerts(
            @RequestParam(defaultValue = "true") boolean includeRecovered) {
        return Map.of("alerts", alertService.listAlerts(includeRecovered));
    }

    @GetMapping("/notifications")
    public Map<String, List<AlertNotification>> notifications() {
        return Map.of("notifications", alertService.notifications());
    }
}

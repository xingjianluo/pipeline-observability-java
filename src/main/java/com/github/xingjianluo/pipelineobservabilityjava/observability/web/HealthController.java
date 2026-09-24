package com.github.xingjianluo.pipelineobservabilityjava.observability.web;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthSnapshot;
import com.github.xingjianluo.pipelineobservabilityjava.observability.service.EvaluationEngine;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 健康判定：GET 查询最近一轮快照前会先实时评估一次；POST /api/health/tick 手动驱动一轮评估。
 */
@RestController
@RequestMapping("/api/health")
public class HealthController {

    private final EvaluationEngine engine;

    public HealthController(EvaluationEngine engine) {
        this.engine = engine;
    }

    @GetMapping
    public Map<String, HealthSnapshot> all() {
        return engine.tick();
    }

    @GetMapping("/{pipelineId}")
    public HealthSnapshot one(@PathVariable String pipelineId) {
        return engine.tick().get(pipelineId);
    }

    @PostMapping("/tick")
    public Map<String, HealthSnapshot> tick() {
        return engine.tick();
    }
}

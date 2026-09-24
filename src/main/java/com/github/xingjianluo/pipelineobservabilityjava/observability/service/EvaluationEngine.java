package com.github.xingjianluo.pipelineobservabilityjava.observability.service;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.HealthSnapshot;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineRuntime;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.PipelineRegistry;
import com.github.xingjianluo.pipelineobservabilityjava.observability.util.MutableClock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 周期性评估引擎：对所有已注册管道做健康判定，并把结论交给 {@link AlertService} 协调
 * 触发/升级/恢复/抑制。单个管道“判定+告警协调”顺序执行，整体无共享可变中间态。
 */
@Service
public class EvaluationEngine {

    private static final Logger log = LoggerFactory.getLogger(EvaluationEngine.class);

    private final PipelineRegistry registry;
    private final HealthEvaluator healthEvaluator;
    private final AlertService alertService;
    private final MutableClock clock;

    public EvaluationEngine(PipelineRegistry registry, HealthEvaluator healthEvaluator,
                            AlertService alertService, MutableClock clock) {
        this.registry = registry;
        this.healthEvaluator = healthEvaluator;
        this.alertService = alertService;
        this.clock = clock;
    }

    public Map<String, HealthSnapshot> tick() {
        long now = clock.nowMillis();
        List<PipelineRuntime> pipelines = registry.all();
        Map<String, HealthSnapshot> snapshots = new LinkedHashMap<>();
        for (PipelineRuntime runtime : pipelines) {
            HealthSnapshot snapshot = healthEvaluator.evaluate(runtime, now);
            alertService.reconcile(snapshot, runtime.config().escalationAfterMs());
            snapshots.put(runtime.pipelineId(), snapshot);
        }
        log.info("EvaluationEngine.tick | at={} pipelines={} conclusions={}", now, pipelines.size(),
                snapshots.values().stream().collect(
                        LinkedHashMap::new,
                        (m, s) -> m.put(s.pipelineId(), s.status()),
                        LinkedHashMap::putAll));
        return snapshots;
    }
}

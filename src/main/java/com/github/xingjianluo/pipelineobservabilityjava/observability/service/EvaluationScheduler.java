package com.github.xingjianluo.pipelineobservabilityjava.observability.service;

import com.github.xingjianluo.pipelineobservabilityjava.observability.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityjava.observability.util.MutableClock;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 固定延迟驱动 {@link EvaluationEngine#tick()}。
 * 可用 {@code pipeline.observability.scheduler-enabled=false} 关闭（测试中手动 tick 更可控）。
 */
@Component
public class EvaluationScheduler {

    private static final Logger log = LoggerFactory.getLogger(EvaluationScheduler.class);

    private final EvaluationEngine engine;
    private final ObservabilityProperties properties;

    public EvaluationScheduler(EvaluationEngine engine, ObservabilityProperties properties,
                               MutableClock clock) {
        this.engine = engine;
        this.properties = properties;
    }

    @PostConstruct
    public void start() {
        log.info("EvaluationScheduler.init | enabled={} intervalMs={}",
                properties.isSchedulerEnabled(), properties.getEvaluationIntervalMs());
    }

    @PreDestroy
    public void stop() {
        log.info("EvaluationScheduler.stop");
    }

    @Scheduled(fixedDelayString = "${pipeline.observability.evaluation-interval-ms:5000}")
    public void scheduledTick() {
        if (!properties.isSchedulerEnabled()) {
            return;
        }
        engine.tick();
    }
}

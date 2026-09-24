package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.List;

import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;

/** 新鲜度 / 调度滞后 / SLA 判定。 */
public interface HealthEvaluationService {

    HealthAssessment assess(String pipelineName, long atMillis);

    /** 评估并把结论同步给告警服务（触发/恢复），返回评估结果。 */
    HealthAssessment assessAndReconcile(String pipelineName, long atMillis);

    List<HealthAssessment> assessAll(long atMillis);
}

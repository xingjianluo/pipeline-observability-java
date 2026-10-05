package com.github.xingjianluo.pipelineobservabilityjava.service;

import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineReconciliation;

/** 运行质量与告警的对账：同一时刻给出健康结论、告警状态与各类计数，口径一致。 */
public interface ReconciliationService {

    /** 以当前时钟对指定管道做一次对账（先按该时刻完成健康评估与告警对账，再取计数快照）。 */
    PipelineReconciliation reconcile(String pipelineName);
}

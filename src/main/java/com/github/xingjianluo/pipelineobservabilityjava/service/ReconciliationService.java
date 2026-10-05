package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.List;

import com.github.xingjianluo.pipelineobservabilityjava.model.ReconciliationReport;

/** 运行质量与上报告警对账：同口径取健康结论、告警状态与计数。 */
public interface ReconciliationService {

    /** 单管道对账：健康判定 + 告警对账 + 计数快照在同一临界区内完成。 */
    ReconciliationReport report(String pipelineName, long atMillis);

    /** 全部管道对账（按管道名排序）。 */
    List<ReconciliationReport> reportAll(long atMillis);
}

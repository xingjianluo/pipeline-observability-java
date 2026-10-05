package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.List;

import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.HealthAssessment;

/** 告警触发、去重合并、升级、恢复与维护窗口抑制。 */
public interface AlertService {

    /** 依据一次健康评估同步告警状态（幂等、可并发）。 */
    Alert reconcile(HealthAssessment assessment, long atMillis);

    /** 运行失败事件直接触发 RUN_FAILED 告警。 */
    Alert reportFailure(String pipelineName, String runId, long atMillis, String detail);

    /** 显式恢复某类告警。 */
    Alert recover(String pipelineName, AlertType type, long atMillis);

    List<Alert> list(boolean includeRecovered);

    /** 该管道当前所有活跃告警的合并观测次数之和（告警合并计数）。 */
    long mergedCountForPipeline(String pipelineName);

    /** 该管道当前活跃告警在维护窗口内被抑制的合并次数之和。 */
    long suppressedCountForPipeline(String pipelineName);

    /** 该管道当前活跃告警已对外发送的通知次数之和。 */
    long notifiedCountForPipeline(String pipelineName);
}

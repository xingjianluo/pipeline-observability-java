package com.github.xingjianluo.pipelineobservabilityjava.store;

import java.util.List;

import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineMetrics;

/**
 * 管道接入计数器：乱序/重复/迟到/并发上报下精确累加，不丢不重。
 * 计数在各 run 的收敛临界区或拒绝路径上原子发生。
 */
public interface PipelineMetricsRegistry {

    /** 收到一条上报（无论后续结果如何，每个 HTTP 上报恰好计一次）。 */
    void incrementReceived(String pipelineName);

    /** 事件被真正应用，推进了运行状态。 */
    void incrementAccepted(String pipelineName);

    /** 精确重复。 */
    void incrementDuplicate(String pipelineName);

    /** 迟到被忽略。 */
    void incrementLate(String pipelineName);

    /** 被拒（原因留痕由 RejectionLedger 负责，这里只计数）。 */
    void incrementRejected(String pipelineName);

    /** 取某管道计数的一致性快照。 */
    PipelineMetrics snapshot(String pipelineName);

    /** 全部管道计数快照（按管道名排序）。 */
    List<PipelineMetrics> snapshots();
}

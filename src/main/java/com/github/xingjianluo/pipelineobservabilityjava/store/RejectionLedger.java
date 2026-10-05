package com.github.xingjianluo.pipelineobservabilityjava.store;

import java.util.List;
import java.util.Map;

import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectedEvent;

/** 被拒上报留痕：可按管道/原因查询明细，并给出每种原因的累计计数。 */
public interface RejectionLedger {

    /** 追加一条拒绝留痕（并发安全）。 */
    void record(RejectedEvent event);

    /** 查询留痕明细；pipelineName 为 null 表示不限管道，code 为 null 表示不限原因。 */
    List<RejectedEvent> query(String pipelineName, RejectCode code);

    /** 某管道每种拒绝原因的累计计数；pipelineName 为 null 表示全量。 */
    Map<RejectCode, Long> countByReason(String pipelineName);

    /** 某管道被拒总数。 */
    long totalCount(String pipelineName);
}

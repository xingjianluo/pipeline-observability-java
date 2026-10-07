package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.List;
import java.util.Map;

import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectionRecord;

/** 被拒上报的留痕与分类查询：每一次拒绝都记录，可按管道/原因查询并给出各原因累计计数。
 *  明细有容量上限（超出按到达先后淘汰最旧），但累计计数始终精确、不随淘汰减少。 */
public interface RejectionLogService {

    /** 记录一次拒绝（线程安全，并发下计数不丢失）。pipelineName 无法解析时传 null。 */
    RejectionRecord record(String pipelineName, RejectCode code, String message, String rawSnippet);

    /** 按管道与原因过滤查询当前保留的明细，参数为 null 表示不过滤，按记录顺序返回。 */
    List<RejectionRecord> query(String pipelineName, RejectCode code);

    /** 各原因累计被拒条数（含已被淘汰的明细）；pipelineName 为 null 时统计全部管道。 */
    Map<RejectCode, Long> countByCode(String pipelineName);

    /** 某管道累计被拒总条数（含已被淘汰的明细）；pipelineName 为 null 时返回全局总数。 */
    long totalCount(String pipelineName);

    /** 明细容量上限（条）。 */
    int capacity();

    /** 当前保留的明细条数（<= capacity）。 */
    int retainedCount();

    /** 累计被淘汰的明细条数（到达最早、超出容量被移除）。 */
    long evictedCount();
}

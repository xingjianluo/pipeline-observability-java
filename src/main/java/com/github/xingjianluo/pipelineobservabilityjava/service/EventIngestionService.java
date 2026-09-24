package com.github.xingjianluo.pipelineobservabilityjava.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.xingjianluo.pipelineobservabilityjava.model.IngestResult;

/** 事件接入：解析 + 校验（缺失/类型/协议版本）+ 状态收敛。 */
public interface EventIngestionService {

    IngestResult ingest(JsonNode raw);
}

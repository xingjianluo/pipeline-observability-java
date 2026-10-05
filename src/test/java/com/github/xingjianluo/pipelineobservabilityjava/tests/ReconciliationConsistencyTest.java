package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineReconciliation;
import com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.ReconciliationService;
import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/**
 * 对账自洽：同一时刻的健康结论与告警状态同口径（正常则无活跃告警、异常则告警在挂），
 * 接收/重复/迟到/被拒/告警合并计数与实际上报逐条对得上。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
class ReconciliationConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationConsistencyTest.class);
    private static final String P = "recon-1";
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private ReconciliationService reconciliation;
    @Autowired
    private SystemClock clock;

    private void send(String json) throws Exception {
        log.info("[INPUT] {}", json);
        var r = ingestion.ingest(mapper.readTree(json));
        log.info("[RESULT] accepted={} dup={} code={} msg={}",
                r.isAccepted(), r.isDuplicate(), r.getRejectCode(), r.getMessage());
    }

    private String ev(String runId, String status, long t) {
        return "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\"" + P
                + "\",\"status\":\"" + status + "\",\"eventTime\":" + t + "}";
    }

    @Test
    void healthAlertsAndCountersAreConsistent() throws Exception {
        // 事件时间锚定真实当前时刻，测试时钟拨到同一基准，保证判定确定。
        long base = System.currentTimeMillis();
        ((MutableClock) clock).set(base);
        log.info("[INPUT] base={} freshnessThreshold=3600s jitterGrace=300s", base);

        // run1：正常完成（lastSuccess 停在 base-4.9M，对 base 而言已超期 -> STALE）
        send(ev("rc-1", "RUNNING", base - 5_000_000L));
        send(ev("rc-1", "SUCCESS", base - 4_900_000L));
        send(ev("rc-1", "SUCCESS", base - 4_900_000L)); // 精确重复 -> duplicate
        // run2：PENDING -> RUNNING -> FAILED 终结；随后一条更旧的 PENDING 迟到被忽略
        send(ev("rc-2", "PENDING", base - 4_800_000L));
        send(ev("rc-2", "RUNNING", base - 4_700_000L));
        send(ev("rc-2", "FAILED", base - 4_600_000L));
        send(ev("rc-2", "PENDING", base - 4_750_000L)); // 迟到 -> lateIgnored
        // 两条被拒：未知状态 + 缺字段
        send(ev("rc-x", "EXPLODED", base - 1000L));
        send("{\"protocolVersion\":\"1.0\",\"pipelineName\":\"" + P + "\",\"status\":\"RUNNING\"}");

        // 异常口径：健康结论 STALE，活跃告警必须同口径在挂
        PipelineReconciliation r = reconciliation.reconcile(P);
        log.info("[RECON] verdict={} activeAlerts={} received={} dup={} late={} applied={} "
                        + "rejected={} byCode={} alertMerged={} runs={}",
                r.getHealth().getStatus(),
                r.getActiveAlerts().stream().map(a -> a.getType() + "/" + a.getState()).toList(),
                r.getReceivedEvents(), r.getDuplicateEvents(), r.getLateEvents(),
                r.getAppliedEvents(), r.getRejectedEvents(), r.getRejectionsByCode(),
                r.getAlertMergedCount(), r.getRunsTracked());
        assertEquals(FreshnessStatus.STALE, r.getHealth().getStatus());
        assertTrue(r.getActiveAlerts().stream().anyMatch(a -> a.getType() == AlertType.STALE),
                "STALE verdict must come with an active STALE alert, same viewpoint");
        assertTrue(r.getActiveAlerts().stream().anyMatch(a -> a.getType() == AlertType.RUN_FAILED),
                "failed run alert must still be active while pipeline is unhealthy");
        assertEquals(r.getEvaluatedAt(), r.getHealth().getEvaluatedAt(),
                "health and reconciliation must be evaluated at the same instant");

        // 计数精确对账：7 条接收 = 5 条生效 + 1 重复 + 1 迟到；2 条被拒
        assertEquals(7, r.getReceivedEvents());
        assertEquals(1, r.getDuplicateEvents());
        assertEquals(1, r.getLateEvents());
        assertEquals(5, r.getAppliedEvents());
        assertEquals(2, r.getRejectedEvents());
        assertEquals(1L, r.getRejectionsByCode().get(RejectCode.UNKNOWN_STATUS));
        assertEquals(1L, r.getRejectionsByCode().get(RejectCode.MISSING_FIELD));
        assertEquals(2, r.getRunsTracked());
        long activeCounts = r.getActiveAlerts().stream().mapToLong(a -> a.getCount()).sum();
        assertTrue(r.getAlertMergedCount() >= activeCounts && r.getAlertMergedCount() > 0,
                "merged alert count must cover active alerts");

        // 恢复口径：补跑成功后健康结论转 HEALTHY，活跃告警必须同口径清空，互不矛盾
        send(ev("rc-3", "SUCCESS", base - 1_000_000L));
        PipelineReconciliation ok = reconciliation.reconcile(P);
        log.info("[RECON-RECOVERED] verdict={} activeAlerts={} received={} rejected={}",
                ok.getHealth().getStatus(), ok.getActiveAlerts().size(),
                ok.getReceivedEvents(), ok.getRejectedEvents());
        assertEquals(FreshnessStatus.HEALTHY, ok.getHealth().getStatus());
        assertEquals(0, ok.getActiveAlerts().size(),
                "HEALTHY verdict must not coexist with any active alert");
        assertEquals(8, ok.getReceivedEvents());
        assertEquals(2, ok.getRejectedEvents());
    }
}

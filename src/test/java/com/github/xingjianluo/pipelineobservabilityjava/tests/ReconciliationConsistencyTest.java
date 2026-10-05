package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityjava.model.FreshnessStatus;
import com.github.xingjianluo.pipelineobservabilityjava.model.ReconciliationReport;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.service.ReconciliationService;

/**
 * 对账自洽性：同一时刻查出来的健康结论与告警状态必须同一口径、互不矛盾；
 * 计数（接收/接受/重复/迟到/被拒/告警合并）齐全且守恒。
 */
@SpringBootTest
@Import(ClockTestConfig.class)
@TestPropertySource(properties = {
        "observability.pipelines.rc-lag.schedule-interval-ms=600000",
        "observability.pipelines.rc-lag.schedule-grace-ms=30000"
})
class ReconciliationConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationConsistencyTest.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private static final long T0 = ClockTestConfig.T0;

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private ReconciliationService reconciliation;

    private void event(String pipeline, String runId, String status, long t) {
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\""
                + pipeline + "\",\"status\":\"" + status + "\",\"eventTime\":" + t + "}";
        try {
            ingestion.ingest(mapper.readTree(json));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void badEvent(String pipeline, String tail) {
        String json = "{\"protocolVersion\":\"1.0\",\"runId\":\"x\",\"pipelineName\":\""
                + pipeline + "\"," + tail + "}";
        try {
            ingestion.ingest(mapper.readTree(json));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void logReport(ReconciliationReport r) {
        log.info("[REPORT] pipeline={} health={} consistent={} activeAlerts={} checks={} metrics=[recv={},acc={},dup={},late={},rej={}] alertMerged={} rejectedByReason={}",
                r.getPipelineName(), r.getHealthStatus(), r.isConsistent(),
                r.getActiveAlerts().stream().map(a -> a.getType() + "/" + a.getState()).toList(),
                r.getConsistencyChecks(),
                r.getMetrics().getReceived(), r.getMetrics().getAccepted(),
                r.getMetrics().getDuplicate(), r.getMetrics().getLate(),
                r.getMetrics().getRejected(), r.getAlertMergedCount(), r.getRejectedByReason());
    }

    @Test
    void healthyReportHasNoAlertsAndBalancedCounts() {
        String p = "rc-ok";
        event(p, "ok-1", "SUCCESS", T0);
        // 再来两条重复 + 一条迟到（终结后更旧的非终结）
        event(p, "ok-1", "SUCCESS", T0);
        event(p, "ok-1", "SUCCESS", T0);
        event(p, "ok-1", "RUNNING", T0 - 5000L);
        // 一条被拒
        badEvent(p, "\"status\":\"RUNNING\",\"eventTime\":0");

        ReconciliationReport r = reconciliation.report(p, T0);
        logReport(r);
        assertEquals(FreshnessStatus.HEALTHY, r.getHealthStatus());
        assertTrue(r.isConsistent(), "healthy report must be self-consistent: " + r.getConsistencyChecks());
        assertTrue(r.getActiveAlerts().isEmpty(), "HEALTHY must not carry active alerts");

        // 计数守恒：received = accepted + duplicate + late + rejected
        var m = r.getMetrics();
        assertEquals(5, m.getReceived());
        assertEquals(1, m.getAccepted());
        assertEquals(2, m.getDuplicate());
        assertEquals(1, m.getLate());
        assertEquals(1, m.getRejected());
        assertEquals(m.getReceived(),
                m.getAccepted() + m.getDuplicate() + m.getLate() + m.getRejected());
        assertEquals(1L, r.getRejectedByReason().get(
                com.github.xingjianluo.pipelineobservabilityjava.model.RejectCode.INVALID_TIMESTAMP));
    }

    @Test
    void laggingReportBackedByMatchingAlert() {
        String p = "rc-lag";
        event(p, "lag-1", "SUCCESS", T0);
        // 错过一轮以上（due=T0+600s，宽限 30s）
        ReconciliationReport r = reconciliation.report(p, T0 + 640_000L);
        logReport(r);
        assertEquals(FreshnessStatus.SCHEDULE_LAGGING, r.getHealthStatus());
        assertTrue(r.isConsistent(), "lagging verdict must be backed by a SCHEDULE_LAGGING alert");
        boolean hasLagging = r.getActiveAlerts().stream()
                .anyMatch(a -> a.getType() == AlertType.SCHEDULE_LAGGING);
        assertTrue(hasLagging);
        assertTrue(r.getAlertMergedCount() >= 1L, "merged alert count must reflect the lag");

        // 补跑成功后同一口径查询：回到 HEALTHY 且告警清空
        event(p, "lag-2", "SUCCESS", T0 + 641_000L);
        ReconciliationReport r2 = reconciliation.report(p, T0 + 641_001L);
        logReport(r2);
        assertEquals(FreshnessStatus.HEALTHY, r2.getHealthStatus());
        assertTrue(r2.isConsistent());
        assertFalse(r2.getActiveAlerts().stream()
                .anyMatch(a -> a.getType() == AlertType.SCHEDULE_LAGGING));
    }

    @Test
    void reportAllIncludesOnlyKnownPipelinesAndEachIsConsistent() {
        event("rc-all1", "all-1", "SUCCESS", T0);
        var reports = reconciliation.reportAll(T0);
        boolean allConsistent = reports.stream().allMatch(ReconciliationReport::isConsistent);
        log.info("[REPORT ALL] count={} allConsistent={} pipelines={}",
                reports.size(), allConsistent,
                reports.stream().map(ReconciliationReport::getPipelineName).toList());
        assertTrue(allConsistent, "every pipeline report must be internally consistent");
        // 未知管道桶不出现在全量对账中
        assertFalse(reports.stream()
                .anyMatch(r -> "_unknown_".equals(r.getPipelineName())));
    }
}

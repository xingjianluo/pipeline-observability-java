package com.github.xingjianluo.pipelineobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** 端到端 HTTP：事件接入、健康查询、维护窗口、畸形 JSON 拒绝。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(ClockTestConfig.class)
class WebApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(WebApiIntegrationTest.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @LocalServerPort
    private int port;
    @Autowired
    private TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void endToEndReportAndQuery() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        long t = ClockTestConfig.T0;
        String body = "{\"protocolVersion\":\"1.1\",\"runId\":\"web-1\",\"pipelineName\":\"web-p\","
                + "\"status\":\"SUCCESS\",\"eventTime\":" + t + "}";
        log.info("[HTTP POST] /api/observability/events body={}", body);
        String resp = rest.postForObject(url("/api/observability/events"),
                new HttpEntity<>(body, headers), String.class);
        JsonNode node = mapper.readTree(resp);
        log.info("[HTTP RESP] {}", resp);
        assertTrue(node.get("accepted").asBoolean());
        assertEquals("SUCCESS", node.get("currentStatus").asText());

        String health = rest.getForObject(url("/api/observability/health/web-p"), String.class);
        JsonNode h = mapper.readTree(health);
        log.info("[HEALTH] {}", health);
        assertEquals("HEALTHY", h.get("status").asText());

        String alerts = rest.getForObject(
                url("/api/observability/alerts?includeRecovered=true"), String.class);
        log.info("[ALERTS] {}", alerts);
        assertTrue(mapper.readTree(alerts).isArray());
    }

    @Test
    void malformedJsonIsRejectedWithCodeNotSilentlyDropped() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String resp = rest.postForObject(url("/api/observability/events"),
                new HttpEntity<>("{not-json", headers), String.class);
        log.info("[MALFORMED RESP] {}", resp);
        // 整条报文格式不合法是独立原因，与字段级 TYPE_MISMATCH 区分开
        assertTrue(resp.contains("MALFORMED_JSON"), "malformed body must carry reject code");
    }

    @Test
    void invalidEventRejected() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"protocolVersion\":\"2.0\",\"runId\":\"x\",\"pipelineName\":\"p\","
                + "\"status\":\"RUNNING\",\"eventTime\":1700000000000}";
        String resp = rest.postForObject(url("/api/observability/events"),
                new HttpEntity<>(body, headers), String.class);
        log.info("[BAD PROTOCOL RESP] {}", resp);
        assertTrue(resp.contains("UNSUPPORTED_PROTOCOL_VERSION"));
        assertFalse(resp.contains("\"accepted\":true"));
    }

    @Test
    void rejectionsAreQueryableByPipelineAndCodeOverHttp() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        // 同一管道制造两类拒绝：未知状态 + 不支持的协议版本
        String bad1 = "{\"protocolVersion\":\"1.0\",\"runId\":\"wr-1\",\"pipelineName\":\"web-rej\","
                + "\"status\":\"WHAT\",\"eventTime\":1700000000000}";
        String bad2 = "{\"protocolVersion\":\"7.7\",\"runId\":\"wr-2\",\"pipelineName\":\"web-rej\","
                + "\"status\":\"RUNNING\",\"eventTime\":1700000000000}";
        rest.postForObject(url("/api/observability/events"), new HttpEntity<>(bad1, headers), String.class);
        rest.postForObject(url("/api/observability/events"), new HttpEntity<>(bad2, headers), String.class);

        String list = rest.getForObject(
                url("/api/observability/rejections?pipeline=web-rej&code=UNKNOWN_STATUS"), String.class);
        log.info("[REJECTIONS] {}", list);
        JsonNode arr = mapper.readTree(list);
        assertEquals(1, arr.size());
        assertEquals("UNKNOWN_STATUS", arr.get(0).get("rejectCode").asText());
        assertEquals("web-rej", arr.get(0).get("pipelineName").asText());

        String summary = rest.getForObject(
                url("/api/observability/rejections/summary?pipeline=web-rej"), String.class);
        log.info("[REJECTION SUMMARY] {}", summary);
        JsonNode s = mapper.readTree(summary);
        assertEquals(2, s.get("total").asInt());
        assertEquals(1, s.get("byCode").get("UNKNOWN_STATUS").asInt());
        assertEquals(1, s.get("byCode").get("UNSUPPORTED_PROTOCOL_VERSION").asInt());
        assertEquals(0, s.get("byCode").get("MALFORMED_JSON").asInt());
    }

    @Test
    void reconciliationEndpointGivesConsistentView() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        long t = System.currentTimeMillis();
        String ok = "{\"protocolVersion\":\"1.0\",\"runId\":\"wrc-1\",\"pipelineName\":\"web-recon\","
                + "\"status\":\"SUCCESS\",\"eventTime\":" + t + "}";
        String dup = ok;
        String bad = "{\"protocolVersion\":\"1.0\",\"runId\":\"wrc-2\",\"pipelineName\":\"web-recon\","
                + "\"status\":\"NOPE\",\"eventTime\":" + t + "}";
        rest.postForObject(url("/api/observability/events"), new HttpEntity<>(ok, headers), String.class);
        rest.postForObject(url("/api/observability/events"), new HttpEntity<>(dup, headers), String.class);
        rest.postForObject(url("/api/observability/events"), new HttpEntity<>(bad, headers), String.class);

        String recon = rest.getForObject(
                url("/api/observability/pipelines/web-recon/reconciliation"), String.class);
        log.info("[RECONCILIATION] {}", recon);
        JsonNode r = mapper.readTree(recon);
        assertEquals("web-recon", r.get("pipelineName").asText());
        assertEquals("HEALTHY", r.get("health").get("status").asText());
        // 同一口径：HEALTHY 时不得挂任何活跃告警
        assertEquals(0, r.get("activeAlerts").size());
        assertEquals(2, r.get("receivedEvents").asInt());
        assertEquals(1, r.get("duplicateEvents").asInt());
        assertEquals(1, r.get("appliedEvents").asInt());
        assertEquals(1, r.get("rejectedEvents").asInt());
        assertEquals(1, r.get("rejectionsByCode").get("UNKNOWN_STATUS").asInt());
    }
}

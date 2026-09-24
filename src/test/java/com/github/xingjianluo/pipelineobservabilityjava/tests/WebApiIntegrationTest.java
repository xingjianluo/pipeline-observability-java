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
        assertTrue(resp.contains("TYPE_MISMATCH"), "malformed body must carry reject code");
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
}

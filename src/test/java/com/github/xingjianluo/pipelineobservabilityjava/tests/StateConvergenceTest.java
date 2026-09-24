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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityjava.model.IngestResult;
import com.github.xingjianluo.pipelineobservabilityjava.model.PipelineRunState;
import com.github.xingjianluo.pipelineobservabilityjava.model.RunStatus;
import com.github.xingjianluo.pipelineobservabilityjava.service.EventIngestionService;
import com.github.xingjianluo.pipelineobservabilityjava.store.RunStateStore;

/** 乱序、重复上报下的状态机收敛：结论一致、终结态不回退。 */
@SpringBootTest
@Import(ClockTestConfig.class)
class StateConvergenceTest {

    private static final Logger log = LoggerFactory.getLogger(StateConvergenceTest.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EventIngestionService ingestion;
    @Autowired
    private RunStateStore store;

    private String event(String runId, String status, long eventTime) {
        return "{\"protocolVersion\":\"1.0\",\"runId\":\"" + runId + "\",\"pipelineName\":\"conv\","
                + "\"status\":\"" + status + "\",\"eventTime\":" + eventTime + "}";
    }

    private IngestResult send(String json) {
        try {
            log.info("[INPUT] {}", json);
            IngestResult r = ingestion.ingest(mapper.readTree(json));
            log.info("[RESULT] accepted={} dup={} status={} msg={}",
                    r.isAccepted(), r.isDuplicate(), r.getCurrentStatus(), r.getMessage());
            return r;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void duplicateEventsMergeWithoutChange() {
        String json = event("dup-1", "RUNNING", 1_700_000_001_000L);
        IngestResult first = send(json);
        IngestResult second = send(json);
        IngestResult third = send(json);
        assertTrue(first.isAccepted() && !first.isDuplicate());
        assertTrue(second.isAccepted() && second.isDuplicate());
        assertTrue(third.isAccepted() && third.isDuplicate());
        PipelineRunState s = store.find("dup-1").orElseThrow();
        assertEquals(3, s.getEventCount());
        assertEquals(2, s.getDuplicateCount());
        assertEquals(RunStatus.RUNNING, s.getStatus());
        log.info("[ASSERT] eventCount=3 duplicateCount=2 status={}", s.getStatus());
    }

    @Test
    void outOfOrderTerminalThenLateNonTerminalDoesNotRollBack() {
        // 乱序：先到 FAILED（终结），后到更早的 RUNNING，状态不得回退。
        send(event("ooo-1", "FAILED", 1_700_000_003_000L));
        IngestResult late = send(event("ooo-1", "RUNNING", 1_700_000_001_000L));
        PipelineRunState s = store.find("ooo-1").orElseThrow();
        assertEquals(RunStatus.FAILED, s.getStatus());
        assertEquals(1, s.getLateIgnoredCount());
        assertFalse(late.isDuplicate());
        assertTrue(late.getMessage().contains("late event ignored"));
        log.info("[ASSERT] status stays FAILED, lateIgnored=1, log={}", s.getTransitionLog());
    }

    @Test
    void outOfOrderArrivalConvergesToFinalConclusion() {
        // 到达顺序 RUNNING -> SUCCESS -> FAILED(更晚) => 最终结论取事件时间最新的 SUCCESS? 不：
        // FAILED 事件时间更晚 => 最终为 FAILED；随后更早的 SUCCESS 不得回退。
        send(event("ooo-2", "RUNNING", 1_700_000_001_000L));
        send(event("ooo-2", "SUCCESS", 1_700_000_002_000L));
        send(event("ooo-2", "FAILED", 1_700_000_004_000L));
        PipelineRunState s1 = store.find("ooo-2").orElseThrow();
        assertEquals(RunStatus.FAILED, s1.getStatus());
        // 终结后再到更旧的 SUCCESS：保持 FAILED
        IngestResult ignored = send(event("ooo-2", "SUCCESS", 1_700_000_002_500L));
        PipelineRunState s2 = store.find("ooo-2").orElseThrow();
        assertEquals(RunStatus.FAILED, s2.getStatus());
        assertEquals(1, s2.getLateIgnoredCount());
        log.info("[ASSERT] final={} lateIgnored={} msg={}", s2.getStatus(),
                s2.getLateIgnoredCount(), ignored.getMessage());
    }

    @Test
    void outOfOrderNonTerminalStartStillCaptured() {
        // RUNNING 事件晚到（先收到同次运行的更早 PENDING），起点仍取 RUNNING 的事件时间。
        send(event("ooo-3", "PENDING", 1_700_000_005_000L));
        send(event("ooo-3", "RUNNING", 1_700_000_001_000L));
        PipelineRunState s = store.find("ooo-3").orElseThrow();
        assertEquals(RunStatus.RUNNING, s.getStatus());
        assertEquals(1_700_000_001_000L, s.getStartTime());
        log.info("[ASSERT] startTime captured at {} despite late arrival", s.getStartTime());
    }
}

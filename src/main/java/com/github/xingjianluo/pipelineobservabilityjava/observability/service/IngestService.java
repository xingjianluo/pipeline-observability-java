package com.github.xingjianluo.pipelineobservabilityjava.observability.service;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.IngestResult;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineEvent;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RejectionRecord;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.PipelineRegistry;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.RunStateStore;
import com.github.xingjianluo.pipelineobservabilityjava.observability.util.MutableClock;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 事件接入入口。
 *
 * <p>流程：校验（拒绝即留痕并抛出，绝不静默丢弃） -> {@link RunStateStore} 收敛 run 状态
 * -> {@link PipelineRegistry} 原子更新管道聚合态。并发上报下两个存储均以 compute 原子推进，
 * 对同一事件重复/迟到上报返回幂等结果。</p>
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    /** 拒绝留痕最多保留条数（进程内环形缓冲）。 */
    private static final int MAX_REJECTIONS = 500;

    private final EventValidator validator;
    private final RunStateStore runStateStore;
    private final PipelineRegistry pipelineRegistry;
    private final MutableClock clock;
    private final ConcurrentLinkedDeque<RejectionRecord> rejections = new ConcurrentLinkedDeque<>();

    public IngestService(EventValidator validator, RunStateStore runStateStore,
                         PipelineRegistry pipelineRegistry, MutableClock clock) {
        this.validator = validator;
        this.runStateStore = runStateStore;
        this.pipelineRegistry = pipelineRegistry;
        this.clock = clock;
    }

    public IngestResult ingest(String rawJson) {
        long receivedAt = clock.nowMillis();
        PipelineEvent event;
        try {
            event = validator.validate(rawJson, receivedAt);
        } catch (EventRejectedException ex) {
            recordRejection(ex, rawJson);
            throw ex;
        }

        pipelineRegistry.ensureRegistered(event.pipelineId(), receivedAt);
        RunStateStore.IngestOutcome outcome = runStateStore.apply(event);
        pipelineRegistry.applyRunState(outcome.state(), receivedAt);

        log.info("IngestService.ingest | pipelineId={} runId={} outcome={} status={}",
                event.pipelineId(), event.runId(), outcome.outcome(), outcome.state().status());
        return new IngestResult(outcome.outcome(), event.pipelineId(), event.runId(),
                outcome.state().status(), null, null);
    }

    private void recordRejection(EventRejectedException ex, String rawJson) {
        RejectionRecord record = new RejectionRecord(clock.nowMillis(), ex.reason(),
                ex.getMessage(), abbreviate(rawJson));
        rejections.addFirst(record);
        while (rejections.size() > MAX_REJECTIONS) {
            rejections.removeLast();
        }
        log.warn("IngestService.rejected | reason={} detail={} payload={}",
                ex.reason(), ex.getMessage(), record.rawPayload());
    }

    public List<RejectionRecord> recentRejections(int limit) {
        return rejections.stream().limit(Math.max(1, limit)).toList();
    }

    private static String abbreviate(String raw) {
        if (raw == null) {
            return null;
        }
        return raw.length() <= 512 ? raw : raw.substring(0, 512) + "...(truncated)";
    }
}

package com.github.xingjianluo.pipelineobservabilityjava.observability.service;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.MaintenanceWindow;
import com.github.xingjianluo.pipelineobservabilityjava.observability.util.MutableClock;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 维护窗口管理（进程内状态）。
 *
 * <p>抑制规则：评估时刻 {@code at} 落在窗口 [startAt, endAt) 内，且窗口为全局（pipelineId=null）
 * 或与该管道精确匹配，则该管道的新触发告警进入 SUPPRESSED：不发送 TRIGGERED 通知，
 * 但告警记录完整保留、计数照常合并，接口仍可追溯。窗口结束后再次触发会按正常流程通知。</p>
 */
@Service
public class MaintenanceService {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceService.class);

    private final CopyOnWriteArrayList<MaintenanceWindow> windows = new CopyOnWriteArrayList<>();
    private final AtomicLong idSequence = new AtomicLong();
    private final MutableClock clock;

    public MaintenanceService(MutableClock clock) {
        this.clock = clock;
    }

    public MaintenanceWindow openWindow(String pipelineId, long startAt, long endAt, String reason) {
        if (endAt <= startAt) {
            throw new IllegalArgumentException("maintenance window endAt must be greater than startAt");
        }
        String id = "mw-" + idSequence.incrementAndGet();
        MaintenanceWindow window = new MaintenanceWindow(id, pipelineId, startAt, endAt, reason);
        windows.add(window);
        log.info("MaintenanceService.open | id={} pipelineId={} [{}, {}) reason={}",
                id, pipelineId, startAt, endAt, reason);
        return window;
    }

    public boolean closeWindow(String id) {
        long now = clock.nowMillis();
        boolean removed = windows.removeIf(w -> w.id().equals(id));
        if (removed) {
            log.info("MaintenanceService.close | id={} at={}", id, now);
        }
        return removed;
    }

    /** 指定管道在给定时刻是否处于任一有效维护窗口（全局窗口或精确匹配窗口，边界左闭右开）。 */
    public boolean isSuppressed(String pipelineId, long at) {
        for (MaintenanceWindow w : windows) {
            boolean scopeMatches = w.pipelineId() == null || w.pipelineId().equals(pipelineId);
            boolean timeMatches = at >= w.startAt() && at < w.endAt();
            if (scopeMatches && timeMatches) {
                return true;
            }
        }
        return false;
    }

    public List<MaintenanceWindow> listWindows() {
        return List.copyOf(windows);
    }
}

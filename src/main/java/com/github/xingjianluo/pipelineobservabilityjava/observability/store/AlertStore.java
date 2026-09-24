package com.github.xingjianluo.pipelineobservabilityjava.observability.store;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertNotification;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertRecord;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.AlertType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 告警存储。
 *
 * <ul>
 *   <li>按 key={@code pipelineId:alertType} 去重；{@link #upsert} 原子执行“存在则合并计数/升级/恢复，
 *       不存在则新建”，并发上报下计数不会丢失；</li>
 *   <li>{@link #reconcile} 以同一把锁串行“读-改-写-发通知”，配合 AlertService 保证不重复通知、
 *       恢复后不残留 ACTIVE；</li>
 *   <li>通知流水（触发/升级/恢复）保留在内存中，供接口与测试核对。</li>
 * </ul>
 */
@Component
public class AlertStore {

    private static final Logger log = LoggerFactory.getLogger(AlertStore.class);

    private final ConcurrentHashMap<String, AlertRecord> alerts = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<AlertNotification> notifications = new CopyOnWriteArrayList<>();
    private final Object reconcileLock = new Object();

    public static String key(String pipelineId, AlertType type) {
        return pipelineId + ":" + type.name();
    }

    public Optional<AlertRecord> get(String key) {
        return Optional.ofNullable(alerts.get(key));
    }

    /**
     * 原子 upsert。fn 接收旧记录（不存在为 null），返回新记录；返回 null 表示无变化。
     * 整个读-改-写在 reconcileLock 下执行，保证并发 reconcile 不交叉、不丢更新。
     */
    public AlertRecord upsert(String pipelineId, AlertType type, UnaryOperator<AlertRecord> fn) {
        String key = key(pipelineId, type);
        synchronized (reconcileLock) {
            AlertRecord next = alerts.compute(key, (k, prev) -> {
                AlertRecord computed = fn.apply(prev);
                return computed == null ? prev : computed;
            });
            log.debug("AlertStore.upsert | key={} state={} count={} version={}",
                    key, next.state(), next.count(), next.version());
            return next;
        }
    }

    /**
     * 在同一把锁下执行整块协调逻辑（可跨多个告警类型），保证“读状态->写状态->发通知”原子可见。
     */
    public void reconcile(Runnable operation) {
        synchronized (reconcileLock) {
            operation.run();
        }
    }

    public void appendNotification(AlertNotification notification) {
        notifications.add(notification);
        log.info("AlertStore.notification | key={} kind={} level={} count={} reason={}",
                notification.alertKey(), notification.kind(), notification.level(),
                notification.mergedCount(), notification.reason());
    }

    public List<AlertRecord> all() {
        return new ArrayList<>(alerts.values());
    }

    public List<AlertNotification> notifications() {
        return new ArrayList<>(notifications);
    }
}

package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;
import org.springframework.stereotype.Component;

/** 进程内通知记录器：线程安全，测试可据此断言“不重复通知、恢复无残留”。 */
@Component
public class InMemoryNotificationSink implements NotificationSink {

    public record Notification(String kind, String alertId, String pipeline, String type,
                               String severity, long at) {
        @Override
        public String toString() {
            return kind + "(" + alertId + " " + pipeline + "/" + type + " " + severity + " @ " + at + ")";
        }
    }

    private final CopyOnWriteArrayList<Notification> notifications = new CopyOnWriteArrayList<>();

    @Override
    public void notifyFiring(Alert alert) {
        notifications.add(new Notification("FIRING", alert.getAlertId(), alert.getPipelineName(),
                alert.getType().name(), alert.getSeverity().name(), alert.getLastTriggeredAt()));
    }

    @Override
    public void notifyRecovery(Alert alert) {
        notifications.add(new Notification("RECOVERY", alert.getAlertId(), alert.getPipelineName(),
                alert.getType().name(), alert.getSeverity().name(), alert.getRecoveredAt()));
    }

    @Override
    public void notifyEscalation(Alert alert) {
        notifications.add(new Notification("ESCALATION", alert.getAlertId(), alert.getPipelineName(),
                alert.getType().name(), alert.getSeverity().name(), alert.getLastTriggeredAt()));
    }

    public List<Notification> notifications() {
        return List.copyOf(notifications);
    }

    public List<String> notificationLines() {
        return notifications.stream().map(Notification::toString).toList();
    }
}

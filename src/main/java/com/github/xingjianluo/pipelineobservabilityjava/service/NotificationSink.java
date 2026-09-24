package com.github.xingjianluo.pipelineobservabilityjava.service;

import com.github.xingjianluo.pipelineobservabilityjava.model.Alert;

/** 通知出口（进程内模拟）：记录每次实际发出的通知，便于测试断言不重复通知。 */
public interface NotificationSink {

    void notifyFiring(Alert alert);

    void notifyRecovery(Alert alert);

    void notifyEscalation(Alert alert);
}

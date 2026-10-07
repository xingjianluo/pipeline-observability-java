package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.function.Supplier;

import org.springframework.stereotype.Component;

/**
 * 一致性守卫：上报的“状态收敛 + 联动健康评估/告警对账”与“按管道对账”
 * 在同一把监视器锁内互斥执行，保证一次对账看到的健康结论、告警状态与
 * 各类计数来自同一时刻的一致快照，并发下也不会拿到互相矛盾的组合。
 */
@Component
public class ConsistencyGuard {

    /** 在互斥临界区内执行动作并返回结果（可重入，同线程嵌套安全）。 */
    public <T> T atomically(Supplier<T> action) {
        synchronized (this) {
            return action.get();
        }
    }

    /** 在互斥临界区内执行动作（无返回值）。 */
    public void atomically(Runnable action) {
        synchronized (this) {
            action.run();
        }
    }
}

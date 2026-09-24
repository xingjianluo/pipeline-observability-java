package com.github.xingjianluo.pipelineobservabilityjava.observability.util;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * 进程内可控时钟：
 * <ul>
 *   <li>默认走真实墙钟（{@code fixedNow == Long.MIN_VALUE} 时返回系统时间）；</li>
 *   <li>测试中可 {@link #setNow(long)} 固定到某一时刻，或 {@link #advanceMillis(long)} 推进，
 *       以稳定覆盖时钟边界（恰好等于阈值、刚刚恢复等）场景。</li>
 * </ul>
 */
@Component
public class MutableClock {

    private final AtomicLong fixedNow = new AtomicLong(Long.MIN_VALUE);

    public long nowMillis() {
        long fixed = fixedNow.get();
        return fixed == Long.MIN_VALUE ? System.currentTimeMillis() : fixed;
    }

    /** 固定到指定 epoch ms；之后 nowMillis() 恒定返回该值直到再次调整。 */
    public void setNow(long epochMillis) {
        fixedNow.set(epochMillis);
    }

    /**
     * 推进时钟。若尚未固定（处于真实墙钟模式），先把当前真实时间固定下来再推进，
     * 保证测试语义为“自当前时刻前进 deltaMs”。
     */
    public void advanceMillis(long deltaMs) {
        fixedNow.updateAndGet(prev -> (prev == Long.MIN_VALUE ? System.currentTimeMillis() : prev) + deltaMs);
    }
}

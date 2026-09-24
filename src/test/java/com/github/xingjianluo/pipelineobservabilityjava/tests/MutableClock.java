package com.github.xingjianluo.pipelineobservabilityjava.tests;

import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/** 可固定/拨动的测试时钟，用于精确覆盖时钟边界。 */
public class MutableClock extends SystemClock {
    private volatile long now;

    public MutableClock(long start) {
        this.now = start;
    }

    public void set(long millis) {
        this.now = millis;
    }

    public void advance(long deltaMs) {
        this.now += deltaMs;
    }

    @Override
    public long nowMillis() {
        return now;
    }
}

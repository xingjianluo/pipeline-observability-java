package com.github.xingjianluo.pipelineobservabilityjava.time;

/** 可替换时钟，测试可固定/拨动时间以覆盖时钟边界。 */
public class SystemClock {

    public long nowMillis() {
        return System.currentTimeMillis();
    }
}

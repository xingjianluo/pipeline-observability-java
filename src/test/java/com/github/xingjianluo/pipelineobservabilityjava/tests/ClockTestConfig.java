package com.github.xingjianluo.pipelineobservabilityjava.tests;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

@TestConfiguration
public class ClockTestConfig {

    public static final long T0 = 1_700_000_000_000L;

    @Bean
    @Primary
    public SystemClock mutableClock() {
        return new MutableClock(T0);
    }
}

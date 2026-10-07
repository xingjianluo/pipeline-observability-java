package com.github.xingjianluo.pipelineobservabilityjava.config;

import java.util.concurrent.locks.ReentrantLock;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.xingjianluo.pipelineobservabilityjava.time.SystemClock;

/** Bean 装配入口。 */
@Configuration
@EnableConfigurationProperties(ObservabilityProperties.class)
public class ObservabilityConfig {

    @Bean
    public SystemClock systemClock() {
        return new SystemClock();
    }

    /**
     * 上报联动对账与显式对账共用的互斥锁（公平模式，对账不被上报流饿死）。
     * 同一把锁保证：一次对账里的健康结论、告警状态与各项计数来自同一时刻的一致快照，
     * 上报与对账并发时也不会拼出互相矛盾的组合。
     */
    @Bean
    public ReentrantLock observabilityLock() {
        return new ReentrantLock(true);
    }
}

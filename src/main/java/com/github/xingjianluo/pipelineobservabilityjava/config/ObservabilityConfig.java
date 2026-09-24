package com.github.xingjianluo.pipelineobservabilityjava.config;

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
}

package com.ad.muface.batch.scheduler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RemoteJobConfig {

    @Bean
    public RetryTemplate remoteJobRetryTemplate() {
        return RetryTemplate.builder()
                .maxAttempts(3)
                .fixedBackoff(1000) // 1 segundo entre reintentos
                .retryOn(Exception.class)
                .build();
    }

}

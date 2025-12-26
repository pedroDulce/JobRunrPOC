package com.company.batchscheduler.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class CircuitBreakerConfiguration {

    @Bean
    public CircuitBreaker remoteJobCircuitBreaker(CircuitBreakerRegistry circuitBreakerRegistry) {
        return circuitBreakerRegistry.circuitBreaker("remoteJob");
    }

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Porcentaje de fallos para abrir
                .waitDurationInOpenState(Duration.ofSeconds(10)) // Tiempo en estado abierto
                .slidingWindowSize(5) // Número de llamadas para calcular tasa de fallos
                .permittedNumberOfCallsInHalfOpenState(3)
                .build();

        return CircuitBreakerRegistry.of(config);
    }
}

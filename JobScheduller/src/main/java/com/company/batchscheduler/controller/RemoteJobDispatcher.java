package com.company.batchscheduler.controller;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.function.Supplier;

@Component
@RequiredArgsConstructor
@Slf4j
public class RemoteJobDispatcher {

    private final RestTemplate restTemplate;
    private final CircuitBreaker circuitBreaker;


    public RemoteJobDispatcher() {
        this.restTemplate = new RestTemplate();
        this.circuitBreaker = CircuitBreaker.ofDefaults("remoteJob");
    }

    @Job(name = "Ejecutar job en microservicio de forma síncrona")
    public void executeRestRemote(JobRequest request, JobContext jobContext) {

        Supplier<JobResult> supplier = () -> {
            request.setScheduledAt(LocalDateTime.now());
            ResponseEntity<JobResult> response = restTemplate.postForEntity(
                    request.getParameters().get("url"),
                    request,
                    JobResult.class
            );

            return response.getBody();
        };

        // Con Circuit Breaker
        JobResult result = circuitBreaker.executeSupplier(supplier);

        if (result.getStatus().compareTo(JobStatusEnum.COMPLETED) != 0) {
            throw new RuntimeException("Microservicio reportó error: " +
                    result.getMessage());
        }

        log.info("Job {} ejecutado en microservicio: {}", jobContext.getJobId(), request.getJobType());
    }



}


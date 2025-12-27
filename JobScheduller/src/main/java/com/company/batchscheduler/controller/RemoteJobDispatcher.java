package com.company.batchscheduler.controller;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobType;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

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

    @org.jobrunr.jobs.annotations.Job(name = "Ejecutar job en microservicio de forma síncrona")
    public void executeRestRemote(String jobId, JobType jobType, String microserviceUrl, String parametersJson) {

        Supplier<JobResult> supplier = () -> {
            JobRequest request = new JobRequest(jobId, jobType, parametersJson);
            ResponseEntity<JobResult> response = restTemplate.postForEntity(
                    microserviceUrl,
                    request,
                    JobResult.class
            );

            return response.getBody();
        };

        // Con Circuit Breaker
        JobResult result = circuitBreaker.executeSupplier(supplier);

        if (!result.getSuccess()) {
            throw new RuntimeException("Microservicio reportó error: " +
                    result.getMessage());
        }

        log.info("Job {} ejecutado en microservicio: {}", jobId, jobType);
    }



}


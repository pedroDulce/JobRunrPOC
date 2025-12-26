package org.example.batch.job;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.annotations.Job;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.function.Supplier;

@Component
@Slf4j
public class RemoteJobExecutor {

    private final RestTemplate restTemplate;
    private final CircuitBreaker circuitBreaker;

    public RemoteJobExecutor() {
        this.restTemplate = new RestTemplate();
        this.circuitBreaker = CircuitBreaker.ofDefaults("remoteJob");
    }

    @Job(name = "Ejecutar job en microservicio")
    public void executeInMicroservice(String jobId, String jobType,
                                      String parametersJson) {

        String microserviceUrl = getMicroserviceUrl(jobType);

        Supplier<JobResult> supplier = () -> {
            JobRequest request = new JobRequest(jobId, jobType, parametersJson);

            ResponseEntity<JobResult> response = restTemplate.postForEntity(
                    microserviceUrl + "/api/jobs/execute",
                    request,
                    JobResult.class
            );

            return response.getBody();
        };

        // Con Circuit Breaker
        JobResult result = circuitBreaker.executeSupplier(supplier);

        if (!result.isSuccess()) {
            throw new RuntimeException("Microservicio reportó error: " +
                    result.getMessage());
        }

        log.info("Job {} ejecutado en microservicio: {}", jobId, jobType);
    }

    private String getMicroserviceUrl(String jobType) {
        return switch (jobType) {
            case "customer-summary" -> "http://localhost:8082";
            case "report-generation" -> "http://localhost:8083";
            default -> "http://localhost:8082"; // Default
        };
    }
}


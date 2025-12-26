package com.company.batchscheduler.job;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.annotations.Job;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
@Slf4j
@RequiredArgsConstructor
public class RemoteJobDispatcher {

    private final RestTemplate restTemplate;

    @Job(name = "Ejecutar job remoto", retries = 2)
    public void executeRemoteJob(String jobname, String jobType, String parametersJson,
                                 String microserviceUrl) {

        // RECOMENDACIÓN: colocar la url del microservicio y la expres.cron por variables en el
        // configMaps del micro de arquitectura
        log.info("Despachando job {} a: {}", jobType, microserviceUrl);
        JobRequest request = new JobRequest(jobname, jobType, parametersJson);
        try {
            ResponseEntity<JobResult> response = restTemplate.postForEntity(
                    microserviceUrl + "/api/jobs/execute",
                    request,
                    JobResult.class
            );

            if (response.getStatusCode().is2xxSuccessful() &&
                    response.getBody() != null &&
                    response.getBody().isSuccess()) {
                log.info("✅ Job remoto ejecutado exitosamente");
            } else {
                throw new RuntimeException("Job remoto falló: " +
                        (response.getBody() != null ?
                                response.getBody().getMessage() : "Unknown error"));
            }
        } catch (Exception e) {
            log.error("❌ Error llamando a microservicio: {}", e.getMessage());
            throw e;
        }
    }
}

package com.company.batchscheduler.consumer;

import common.batch.dto.JobResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.JobId;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobResultConsumer {

    private final JobScheduler jobScheduler;

    @KafkaListener(
            topics = "${kafka.topics.job-results}",
            groupId = "${spring.application.name}-results-consumer"
    )
    public void consumeJobResult(
            JobResult result,
            @Header(KafkaHeaders.RECEIVED_KEY) String jobId,
            @Header(value = "jobrunr-job-id", required = false) String jobrunrJobIdHeader) {

        try {
            // Usar jobrunrJobId del header o del objeto
            String jobrunrJobId = jobrunrJobIdHeader != null ?
                    jobrunrJobIdHeader : result.getJobrunrJobId();

            log.info("📨 Received job result: {} for JobRunr Job: {}, Status: {}",
                    jobId, jobrunrJobId, result.getStatus());

            if (jobrunrJobId != null) {
                updateJobRunrStatus(jobrunrJobId, result);
            } else {
                log.warn("No JobRunr Job ID found for job {}", jobId);
            }

        } catch (Exception e) {
            log.error("Error processing job result for {}: {}", jobId, e.getMessage(), e);
        }
    }

    /**
     * Actualizar estado en JobRunr
     */
    private void updateJobRunrStatus(String jobrunrJobId, JobResult result) {
        try {
            // Convertir String a UUID y luego a JobId
            UUID uuid = UUID.fromString(jobrunrJobId);
            JobId jobId = new JobId(uuid);

            switch (result.getStatus()) {

                case ENQUEUED:
                    // JobRunr ya marca jobs como PROCESSING automáticamente
                    log.debug("Job {} is enqueued", jobrunrJobId);
                    break;

                case PUBLISHED:
                    // JobRunr ya marca jobs como PROCESSING automáticamente
                    log.debug("Job {} is published", jobrunrJobId);
                    break;

                case IN_PROGRESS:
                    // JobRunr ya marca jobs como PROCESSING automáticamente
                    log.debug("Job {} is in progress", jobrunrJobId);
                    break;

                case COMPLETED:
                    // JobRunr marca como SUCCEEDED automáticamente al terminar sin excepción
                    log.info("Job {} completed successfully", jobrunrJobId);
                    break;

                case FAILED:
                    // Para fallos, necesitaríamos reintentar o marcar como fallado
                    handleFailedJob(jobId, result);
                    break;

                case CANCELLED:
                    jobScheduler.delete(jobId);
                    log.info("Job {} cancelled", jobrunrJobId);
                    break;

                default:
                    log.warn("Unknown status {} for job {}", result.getStatus(), jobrunrJobId);
            }

        } catch (Exception e) {
            log.error("Failed to update JobRunr status for {}: {}",
                    jobrunrJobId, e.getMessage());
        }
    }

    /**
     * Manejar job fallido
     */
    private void handleFailedJob(JobId jobId, JobResult result) {
        log.error("Job {} failed: {}", jobId, result.getErrorDetails());

        // Podrías:
        // 1. Reintentar el job
        // 2. Notificar administradores
        // 3. Registrar en log de errores

        // Ejemplo: Si no hay más reintentos, dejar que JobRunr lo marque como FAILED
        // JobRunr maneja reintentos automáticamente basado en su configuración
    }
}


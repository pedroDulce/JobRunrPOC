package com.company.batchscheduler.receivenotifier;

import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobId;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.storage.StorageProvider;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobResultConsumer {

    private final StorageProvider storageProvider;
    private final JobScheduler jobScheduler;

    @KafkaListener(
            topics = "${kafka.topics.job-results}",
            groupId = "${spring.application.name}-results-consumer"
    )
    public void consumeJobResult(
            JobResult result,
            @Header(KafkaHeaders.RECEIVED_KEY) String executorJobId,
            @Header(value = "jobrunr-job-id", required = false) String jobrunrJobIdHeader,
            @Header(value = "correlation-id", required = false) String correlationIdHeader) {

        try {
            // Usar jobrunrJobId del header o del objeto
            String jobrunrJobIdStr = jobrunrJobIdHeader != null ?
                    jobrunrJobIdHeader : result.getJobrunrJobId();

            log.info("📨 Received job {} result: {} for JobRunr Job: {}, Status: {}",
                    result.getJobName(), executorJobId, jobrunrJobIdStr, result.getStatus());

            if (jobrunrJobIdStr != null && !jobrunrJobIdStr.isEmpty()) {
                updateJobRunrStatus(jobrunrJobIdStr, result);
            } else {
                log.warn("No JobRunr Job ID found for job {}", jobrunrJobIdStr);
            }

        } catch (Exception e) {
            log.error("Error processing job result for {}: {}", jobrunrJobIdHeader, e.getMessage(), e);
        }
    }

    /**
     * Actualizar estado en JobRunr según tu JobResult
     */
    private void updateJobRunrStatus(String jobrunrJobIdStr, JobResult result) {
        try {
            UUID uuid = UUID.fromString(jobrunrJobIdStr);
            JobId jobId = new JobId(uuid);

            // ⚠️ EN JobRunr 8.3.1: getJobById() devuelve Job o null, NO Optional
            Job job = storageProvider.getJobById(jobId);

            if (job != null) {
                log.debug("Found JobRunr job {} with state: {}", jobId, job.getState());

                switch (result.getStatus()) {
                    case IN_PROGRESS:
                        // JobRunr ya debería estar en PROCESSING
                        handleInProgress(job, result);
                        break;

                    case COMPLETED:
                        // Marcar como exitoso
                        handleCompleted(job, result);
                        break;

                    case FAILED:
                        // Marcar como fallido
                        handleFailed(job, result);
                        break;

                    case CANCELLED:
                        // Eliminar de JobRunr
                        handleCancelled(jobId);
                        break;

                    default:
                        log.warn("Unknown status {} for job {}", result.getStatus(), jobId);
                }
            } else {
                log.warn("JobRunr job {} not found in storage", jobId);
                // Podrías crear un job de tracking retrospectivo si es necesario
                createRetrospectiveJob(jobId, result);
            }

        } catch (IllegalArgumentException e) {
            log.error("Invalid JobRunr Job ID format: {} - {}", jobrunrJobIdStr, e.getMessage());
        } catch (Exception e) {
            log.error("Failed to update JobRunr status for {}: {}",
                    jobrunrJobIdStr, e.getMessage(), e);
        }
    }

    /**
     * Manejar estado IN_PROGRESS
     */
    private void handleInProgress(Job job, JobResult result) {

        UUID jobUuid = job.getId();
        JobId jobId = new JobId(jobUuid);

        // En JobRunr, un job pasa automáticamente a PROCESSING cuando se ejecuta
        // Podemos verificar y loggear
        if (job.getState() == org.jobrunr.jobs.states.StateName.PROCESSING) {
            log.debug("Job {} is already PROCESSING in JobRunr", jobId);
        } else {
            log.info("Job {} is IN_PROGRESS but JobRunr state is: {}",
                    jobId, job.getState());
        }

        // Actualizar metadata si es necesario
        updateJobMetadata(job, "executorStatus", "IN_PROGRESS");
        updateJobMetadata(job, "executorMessage", result.getMessage());
    }

    /**
     * Manejar estado COMPLETED
     */
    private void handleCompleted(Job job, JobResult result) {

        UUID jobUuid = job.getId();
        JobId jobId = new JobId(jobUuid);

        log.info("✅ Job {} completed successfully - {}", jobId, result.getMessage());

        // En JobRunr, el job dummy ya terminó (SUCCEEDED)
        // Podemos actualizar metadata con el resultado real
        updateJobMetadata(job, "executorStatus", "COMPLETED");
        updateJobMetadata(job, "executorResult", result);
        updateJobMetadata(job, "completedAt", result.getCompletedAt());
        updateJobMetadata(job, "processingTime", result.getDurationMs());

        // Si el job en JobRunr aún está en PROCESSING (no debería), forzar éxito
        if (job.getState() == org.jobrunr.jobs.states.StateName.PROCESSING) {
            log.warn("Job {} is still PROCESSING in JobRunr, marking as succeeded", jobId);
            // JobRunr no permite cambiar estado manualmente fácilmente
            // Una opción es re-encolar y hacer que termine exitosamente
            requeueAsSucceeded(jobId, result);
        }
    }

    /**
     * Manejar estado FAILED
     */
    private void handleFailed(Job job, JobResult result) {

        UUID jobUuid = job.getId();
        JobId jobId = new JobId(jobUuid);

        log.error("❌ Job {} failed: {}", jobId, result.getErrorDetails());

        // Actualizar metadata
        updateJobMetadata(job, "executorStatus", "FAILED");
        updateJobMetadata(job, "executorError", "fatal error");
        updateJobMetadata(job, "errorDetails", result.getErrorDetails());

        // Si el job en JobRunr muestra SUCCEEDED (porque el dummy terminó bien),
        // necesitamos marcarlo como fallido
        if (job.getState() == org.jobrunr.jobs.states.StateName.SUCCEEDED) {
            log.warn("Job {} is SUCCEEDED in JobRunr but failed in executor", jobId);
            // Re-encolar y hacer que falle
            requeueAsFailed(jobId, result);
        }
    }

    /**
     * Manejar estado CANCELLED
     */
    private void handleCancelled(JobId jobId) {
        try {
            jobScheduler.delete(jobId);
            log.info("Job {} cancelled", jobId);
        } catch (Exception e) {
            log.error("Failed to cancel job {}: {}", jobId, e.getMessage());
        }
    }

    /**
     * Actualizar metadata del job en JobRunr
     */
    private void updateJobMetadata(Job job, String key, Object value) {
        try {
            // En JobRunr 8.3.1, puedes usar JobDetails para metadata
            // Pero es más complejo. Una alternativa es guardar en tu propia tabla.
            log.debug("Would update metadata for job {}: {}={}",
                    job.getId(), key, value);

        } catch (Exception e) {
            log.warn("Could not update metadata for job {}: {}",
                    job.getId(), e.getMessage());
        }
    }

    /**
     * Re-encolar job como exitoso
     */
    private void requeueAsSucceeded(JobId jobId, JobResult result) {
        try {
            // Eliminar el job actual
            jobScheduler.delete(jobId);

            // Crear nuevo job que termine exitosamente
            JobId newJobId = jobScheduler.enqueue(() -> {
                log.info("Job {} completed successfully in executor",
                        result.getJobId());
                // No hacer nada, solo terminar exitosamente
            });

            log.info("Re-queued job {} as {} with SUCCEEDED state",
                    jobId, newJobId);

        } catch (Exception e) {
            log.error("Failed to requeue job {} as succeeded: {}",
                    jobId, e.getMessage());
        }
    }

    /**
     * Re-encolar job como fallido
     */
    private void requeueAsFailed(JobId jobId, JobResult result) {
        try {
            // Eliminar el job actual
            jobScheduler.delete(jobId);

            // Crear nuevo job que falle explícitamente
            JobId newJobId = jobScheduler.enqueue(() -> {
                log.error("Job {} failed in executor: {}",
                        result.getJobId(), result.getErrorDetails());
                throw new RuntimeException("Job failed in executor: " +
                        result.getErrorDetails());
            });

            log.info("Re-queued job {} as {} with FAILED state",
                    jobId, newJobId);

        } catch (Exception e) {
            log.error("Failed to requeue job {} as failed: {}",
                    jobId, e.getMessage());
        }
    }

    /**
     * Crear job retrospectivo si no existe
     */
    private void createRetrospectiveJob(JobId jobId, JobResult result) {
        try {
            log.info("Creating retrospective job for {}", jobId);

            // Crear un job que refleje el estado real
            if (result.getStatus() == JobStatusEnum.COMPLETED) {
                jobScheduler.enqueue(() -> {
                    log.info("Retrospective: Job {} completed at {}",
                            result.getJobId(), result.getCompletedAt());
                });
            } else if (result.getStatus() == JobStatusEnum.FAILED) {
                jobScheduler.enqueue(() -> {
                    throw new RuntimeException("Retrospective failure: " +
                            result.getErrorDetails());
                });
            }

        } catch (Exception e) {
            log.error("Failed to create retrospective job: {}", e.getMessage());
        }
    }

}

package com.company.batchscheduler.receivenotifier;

import com.company.batchscheduler.service.JobManagementOperations;
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

    private final JobManagementOperations jobManagementOperations;
    private final StorageProvider storageProvider;
    private final JobScheduler jobScheduler;

    @KafkaListener(
            topics = "${kafka.topics.job-results}",
            groupId = "${spring.application.name}-results-consumer"
    )
    public void consumeJobResult(
            JobResult result,
            @Header(KafkaHeaders.RECEIVED_KEY) String executorJobId,
            @Header(value = "jobrunr-job-id", required = false) String jobrunrJobIdHeader) {

        try {
            // Usar jobrunrJobId del header o del objeto
            String jobrunrJobIdStr = jobrunrJobIdHeader != null ?
                    jobrunrJobIdHeader : result.getJobrunrJobId();

            log.info("📨 Received job {} for JobRunr Job ID: {}, result: {}, Status: {}",
                    result.getJobName(), executorJobId,
                    result.getStatus().toString().contentEquals(JobStatusEnum.FAILED.toString()) ? result.getErrorDetails() : result.getMessage(),
                    result.getStatus());

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
            log.info("Job {} is IN_PROGRESS but JobRunr state is: {}", jobId, job.getState());
            jobManagementOperations.startOrContinueJob(job.getId());
        }
    }

    /**
     * Manejar estado COMPLETED
     */
    private void handleCompleted(Job job, JobResult result) {

        UUID jobUuid = job.getId();
        JobId jobId = new JobId(jobUuid);

        log.info("✅ Job {} completed successfully - {}", jobId, result.getMessage());

        // Si el job en JobRunr aún está en PROCESSING, forzar éxito
        if (job.getState() == org.jobrunr.jobs.states.StateName.PROCESSING) {
            log.warn("Job {} is still PROCESSING in JobRunr, marking as succeeded", jobId);
            jobManagementOperations.completeSuccessJob(job.getId(), result);
        }
    }

    /**
     * Manejar estado FAILED
     */
    private void handleFailed(Job job, JobResult result) {

        UUID jobUuid = job.getId();
        JobId jobId = new JobId(jobUuid);

        log.error("❌ Job {} failed: {}", jobId, result.getErrorDetails());

        // El job en JobRunr aún está en PROCESSING o completado por error: marcarlo como fallido
        if (job.getState() == org.jobrunr.jobs.states.StateName.PROCESSING ||
                job.getState() == org.jobrunr.jobs.states.StateName.SUCCEEDED) {
            log.warn("Job {} is PROCESSING in JobRunr but failed in executor", jobId);
            jobManagementOperations.failJob(job.getId(), result);
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

}

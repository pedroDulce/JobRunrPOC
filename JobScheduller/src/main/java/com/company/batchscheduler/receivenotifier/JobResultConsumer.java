package com.company.batchscheduler.receivenotifier;

import com.company.batchscheduler.util.DateTimeUtil;
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

import java.util.ArrayList;
import java.util.List;
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
            @Header(value = "jobrunr-job-id", required = false) String jobrunrJobIdHeader) {

        try {
            // Usar jobrunrJobId presente en el header o el del objeto
            String jobrunrJobIdStr = jobrunrJobIdHeader != null ? jobrunrJobIdHeader : result.getJobId();
            //filtrar ID trabajo, eliminando el ID de instancia:
            int indexHasta = jobrunrJobIdStr.indexOf("instanceId");
            if (indexHasta != -1) {
                jobrunrJobIdStr = jobrunrJobIdStr.substring(0, indexHasta - 1);
            }
            UUID uuid = UUID.fromString(jobrunrJobIdStr == null ? "unknown" : jobrunrJobIdStr);
            Job job = storageProvider.getJobById(new JobId(uuid));
            if (job == null) {
                log.warn("JobRunr job {} not found in storage or was deleted", jobrunrJobIdStr);
            } else {
                log.info("📨 Received job {} for JobRunr Job ID: {}, result: {}, Status: {}",
                        job.getJobName(), executorJobId,
                        result.getStatus().toString().contentEquals(JobStatusEnum.FAILED.toString())
                                ? result.getErrorDetails()
                                : result.getMessage(),
                        result.getStatus());

               updateJobRunrStatus(job, result);

            }

        } catch (Exception e) {
            log.error("Error processing job result for {}: {}", jobrunrJobIdHeader, e.getMessage(), e);
        }
    }

    /**
     * Actualizar estado en JobRunr según tu JobResult
     */
    private void updateJobRunrStatus(Job job, JobResult result) {
        try {
            switch (result.getStatus()) {
                case IN_PROGRESS:
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
                    handleCancelled(job);
                    break;

                default:
                    log.warn("Unknown status {} for job {}", result.getStatus(), job.getId());
            }

        } catch (IllegalArgumentException e) {
            log.error("Invalid JobRunr Job ID format: {} - {}", job.getId(), e.getMessage());
        } catch (Exception e) {
            log.error("Failed to update JobRunr status for {}: {}", job.getId(), e.getMessage(), e);
        }
    }

    /**
     * Manejar estado IN_PROGRESS
     */
    private void handleInProgress(Job job, JobResult jobResult) {

        UUID jobUuid = job.getId();
        JobId jobId = new JobId(jobUuid);

        if (job.getMetadata().get("progress") == null) {
            job.getMetadata().put("progress", 25);
        } else {
            Integer progresoActual = (Integer) job.getMetadata().get("progress");
            if (progresoActual > 95) {
                progresoActual = 99;
            } else {
                progresoActual = progresoActual + 1;
            }
            job.getMetadata().put("progress", progresoActual);
        }
        List<String> existingLabels = new ArrayList<>();
        String jobName = jobResult.getJobName() == null || "".contentEquals(jobResult.getJobName())
                ? (String) job.getMetadata().get("nombre-Job")
                : jobResult.getJobName();
        job.getMetadata().put("nombre-Job", jobName);
        existingLabels.add(jobName.length() > 33 ? jobName.substring(0, 32) : jobName + " EN EJECUCIÓN");
        existingLabels.add("Comienzo: " + DateTimeUtil.formatear(jobResult.getStartedAt()));
        existingLabels.add("Último latido: " + DateTimeUtil.formatNow());

        job.getMetadata().put("lastHeartbeat", DateTimeUtil.formatNow());

        job.setLabels(existingLabels);

        // 3. Guardar
        storageProvider.save(job);
        log.debug("Job {} is already PROCESSING in JobRunr", jobId);

    }

    /**
     * Manejar estado COMPLETED
     */
    private void handleCompleted(Job job, JobResult jobResult) {

        log.info("✅ Job {} completed successfully - {}", job.getId(), jobResult.getMessage());

        job.getMetadata().put("progress", 100);
        job.getMetadata().put("lastHeartbeat", DateTimeUtil.formatNow());
        job.getMetadata().put("finalizado", "De forma exitosa. " + jobResult.getMessage());
        job.getMetadata().put("duracionMs", String.valueOf(jobResult.getDurationMs()));
        job.getMetadata().put("inicio", DateTimeUtil.formatear(jobResult.getStartedAt()));
        job.getMetadata().put("fin", DateTimeUtil.formatear(jobResult.getCompletedAt()));

        String jobName = jobResult.getJobName() == null || "".contentEquals(jobResult.getJobName())
                ? (String) job.getMetadata().get("nombre-Job")
                : jobResult.getJobName();
        job.getMetadata().put("nombre-Job", jobName);
        List<String> existingLabels =  new ArrayList<>();
        existingLabels.add(jobName.length() > 33 ? jobName.substring(0, 32) : jobName + " COMPLETADO");
        existingLabels.add("Finalizado en " + DateTimeUtil.formatNow());
        existingLabels.add("Duración (seg.): " + jobResult.getDurationMs() / 1000);
        job.setLabels(existingLabels);

        // 3. Guardar
        storageProvider.save(job);

    }

    /**
     * Manejar estado FAILED
     */
    private void handleFailed(Job job, JobResult jobResult) {

        log.info("✅ Job {} completed with errors - {}", job.getId(), jobResult.getMessage());
        job.getMetadata().put("progress", 100);

        job.getMetadata().put("finalizado", "Con errores: " + jobResult.getMessage());
        job.getMetadata().put("errorDetails", jobResult.getErrorDetails());
        job.getMetadata().put("duración (ms)", jobResult.getDurationMs());
        job.getMetadata().put("inicio", DateTimeUtil.formatear(jobResult.getStartedAt()));
        job.getMetadata().put("fin", DateTimeUtil.formatear(jobResult.getCompletedAt()));

        String jobName = jobResult.getJobName() == null || "".contentEquals(jobResult.getJobName())
                ? (String) job.getMetadata().get("nombre-Job")
                : jobResult.getJobName();
        job.getMetadata().put("nombre-Job", jobName);

        List<String> existingLabels = job.getLabels();
        existingLabels.add(jobName.length() > 33 ? jobName.substring(0, 32) : jobName + " HA FALLADO");
        existingLabels.add("Error: " + jobResult.getMessage() + ". Detalle Error: " + jobResult.getErrorDetails());
        existingLabels.add("Finalizado en: " + DateTimeUtil.formatear(jobResult.getCompletedAt()));
        job.setLabels(existingLabels);
        // 3. Guardar
        storageProvider.save(job);

    }


    /**
     * Manejar estado CANCELLED
     */
    private void handleCancelled(Job job) {
        try {
            jobScheduler.delete(job.getId());
            log.info("Job {} cancelled", job.getId());
        } catch (Exception e) {
            log.error("Failed to cancel job {}: {}", job.getId(), e.getMessage());
        }
    }


}

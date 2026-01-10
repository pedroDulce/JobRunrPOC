package com.ad.muface.batch.scheduler.service;

import com.ad.muface.batch.dto.JobResult;
import com.ad.muface.batch.scheduler.util.DateTimeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.JobId;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.storage.StorageProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@RequiredArgsConstructor
@Service
@Transactional
public class JobManagementOperations {

    private final StorageProvider storageProvider;

    private final JobScheduler jobScheduler;

    /**
     * Actualizar estado en JobRunr según tu JobResult
     */
    public void updateJobRunrStatus(Job job, JobResult result) {
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

    @Transactional
    public boolean deletePlannedJob(String jobId) {
        try {
            int deleted = storageProvider.deletePermanently(UUID.fromString(jobId));
            log.info("Job {} deleted successfully", jobId);
            return deleted > 0;
        } catch (Exception e) {
            log.error("Error deleting job {}: {}", jobId, e.getMessage());
            return false;
        }
    }

    public Job getJobById(UUID uuid) {
        return this.storageProvider.getJobById(new JobId(uuid));
    }

    /**
     * Obtener información de un job (VERSIÓN CORREGIDA)
     */
    public Map<String, Object> getJobInfo(String jobId) {
        try {
            Job job = storageProvider.getJobById(UUID.fromString(jobId));
            if (job == null) {
                return null;
            }

            JobDetails jobDetails = job.getJobDetails();

            Map<String, Object> jobInfo = new HashMap<>();
            jobInfo.put("id", job.getId().toString());
            jobInfo.put("jobName", jobDetails.getClassName() + "." + jobDetails.getMethodName());
            jobInfo.put("state", job.getState().name());
            jobInfo.put("createdAt", job.getCreatedAt());
            jobInfo.put("updatedAt", job.getUpdatedAt());
            jobInfo.put("jobSignature", jobDetails.getClassName());

            // Obtener scheduledAt si está programado
            if (job.hasState(StateName.AWAITING)) {
                ScheduledState scheduledState = job.getJobState();
                jobInfo.put("scheduledAt", scheduledState.getScheduledAt());
            }

            // Información adicional del job
            jobInfo.put("className", jobDetails.getClassName());
            jobInfo.put("methodName", jobDetails.getMethodName());
            jobInfo.put("jobParameters", jobDetails.getJobParameters());

            return jobInfo;
        } catch (Exception e) {
            log.error("Error getting job info {}: {}", jobId, e.getMessage());
            return null;
        }
    }

    @Transactional
    public boolean deleteRecurringJobByName(String jobName) {
        try {
            int deleted = storageProvider.deleteRecurringJob(jobName);
            log.info("Recurring job {} deleted", jobName);
            return deleted > 0;
        } catch (Exception e) {
            log.error("Error deleting recurring job {}: {}", jobName, e.getMessage());
            return false;
        }
    }


    public String getRecurringIdJobByName(String jobName) {
        try {
            Iterator<RecurringJob> iteRecurringJobs = storageProvider.getRecurringJobs().iterator();
            while (iteRecurringJobs.hasNext()) {
                RecurringJob recurringJob = iteRecurringJobs.next();
                if (recurringJob.getJobName().contentEquals(jobName)) {
                    return recurringJob.toScheduledJob().getId().toString();
                }
            }
            return "inmediately-job";
        } catch (Exception e) {
            log.error("Error searching recurring job {}: {}", jobName, e.getMessage());
            return "inmediately-job";
        }
    }

    /**
     * Manejar estado IN_PROGRESS
     */
    private void handleInProgress(Job job, JobResult jobResult) {

        UUID jobUuid = job.getId();
        JobId jobId = new JobId(jobUuid);

        if (job.getMetadata().get("progress") == null) {
            job.getMetadata().put("progress", 5);
            job.getMetadata().put("comienzo", DateTimeUtil.formatear(jobResult.getStartedAt()));
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
        existingLabels.add("Comienzo: " + job.getMetadata().get("comienzo"));
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

        log.info("✅ Trabajo Remoto {} - {} completado de forma satisfactoria - {}", job.getJobName(),
                job.getId(), jobResult.getMessage());

        job.getMetadata().put("progress", 100);
        job.getMetadata().put("lastHeartbeat", DateTimeUtil.formatNow());
        job.getMetadata().put("finalizado", "De forma exitosa. " + jobResult.getMessage());
        job.getMetadata().put("duracionMs", String.valueOf(jobResult.getExecutionTimeInMills()));
        job.getMetadata().put("fin", DateTimeUtil.formatear(jobResult.getCompletedAt()));

        String jobName = jobResult.getJobName() == null || "".contentEquals(jobResult.getJobName())
                ? (String) job.getMetadata().get("nombre-Job")
                : jobResult.getJobName();
        job.getMetadata().put("nombre-Job", jobName);
        List<String> existingLabels =  new ArrayList<>();
        existingLabels.add(jobName.length() > 33 ? jobName.substring(0, 32) : jobName + " COMPLETADO");
        existingLabels.add("Finalización: " + DateTimeUtil.formatNow());
        existingLabels.add("Duración: " + jobResult.getExecutionTimeInMills() + " ms");
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
        job.getMetadata().put("duración (ms)", jobResult.getExecutionTimeInMills() == null ? "..." : jobResult.getExecutionTimeInMills());
        job.getMetadata().put("fin", DateTimeUtil.formatear(jobResult.getCompletedAt() == null
                ? LocalDateTime.now()
                : jobResult.getCompletedAt()));

        String jobName = jobResult.getJobName() == null || "".contentEquals(jobResult.getJobName())
                ? (String) job.getMetadata().get("nombre-Job")
                : jobResult.getJobName();
        job.getMetadata().put("nombre-Job", jobName);

        List<String> existingLabels = job.getLabels();
        existingLabels.add(jobName.length() > 33 ? jobName.substring(0, 32) : jobName + " HA FALLADO");
        existingLabels.add("Error: " + jobResult.getMessage() + ".Detalle Error: " + jobResult.getErrorDetails());
        existingLabels.add("Finalización: " + DateTimeUtil.formatear(jobResult.getCompletedAt() == null
                ? LocalDateTime.now()
                : jobResult.getCompletedAt()));
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

package com.company.batchscheduler.service;

import common.batch.dto.JobResult;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.storage.StorageProvider;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
@Transactional
public class JobManagementOperations {

    private final StorageProvider storageProvider;

    @PersistenceContext
    private EntityManager entityManager;

    public boolean updateJobStatus(String jobId, Integer progress) {
        // Usar UPDATE con versión para evitar condiciones de carrera
        int updated = entityManager.createNativeQuery("""
            UPDATE jobrunr_jobrunr_jobs 
            SET last_heartbeat = NOW(),
                updated_at = NOW(),
                progress = :progress,
                version = version + 1
            WHERE id = :jobId 
              AND state NOT IN ('SUCCEEDED', 'FAILED', 'DELETED')
              AND (last_heartbeat IS NULL OR last_heartbeat < NOW() - INTERVAL '5 minutes')
            """)
                .setParameter("jobId", jobId)
                .setParameter("progress", progress)
                .executeUpdate();

        if (updated == 0) {
            // Job podría estar completado o tener problemas
            log.debug("Job podría estar completado o tener problemas");
        }
        return updated > 0;
    }


    public boolean completeSuccessJob(Job job, JobResult jobResult) {
        if (job == null) {
            log.error("Job no encontrado");
            return false;
        }
        Job succeededJob = job.succeeded();
        // 2. Añadir metadata (esto sí es mutable)
        succeededJob.getMetadata().put("finalizado", "De forma exitosa. " + jobResult.getMessage());
        succeededJob.getMetadata().put("duracionMs", String.valueOf(jobResult.getDurationMs()));
        succeededJob.getMetadata().put("inicio", jobResult.getStartedAt().toString());
        succeededJob.getMetadata().put("fin", jobResult.getCompletedAt().toString());

        // 3. Persistir el Job completo
        storageProvider.save(succeededJob);

        return true;
    }


    public boolean failJob(Job job, JobResult jobResult) {
        if (job == null) {
            log.error("Job no encontrado");
            return false;
        }
        Job failedJob = job.failed(jobResult.getMessage(), new Exception("Error: " + jobResult.getMessage()
                + ". Detalles: " + jobResult.getErrorDetails()));
        failedJob.getMetadata().put("finalizado", "Con errores: " + jobResult.getMessage());
        failedJob.getMetadata().put("errorDetails", jobResult.getErrorDetails());
        failedJob.getMetadata().put("duración",jobResult.getDurationMs());
        failedJob.getMetadata().put("momento de iniciar",jobResult.getStartedAt());
        failedJob.getMetadata().put("momento de finalización",jobResult.getCompletedAt());

        // 3. Persistir el Job completo
        storageProvider.save(failedJob);

        return true;
    }

    public boolean startOrContinueJob(UUID jobId) {
        return updateJobStatus(jobId.toString(), 50 /*proceso al 50%*/);
    }


    /**
     * Eliminar un job por su ID
     */
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
            //jobInfo.put("labels", jobDetails.getLabels());

            return jobInfo;
        } catch (Exception e) {
            log.error("Error getting job info {}: {}", jobId, e.getMessage());
            return null;
        }
    }

    /**
     * Eliminar un job recurrente por nombre
     */
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



}

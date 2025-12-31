package com.company.batchscheduler.service;

import com.company.batchscheduler.repository.JobRunrAdminRepository;
import common.batch.dto.JobResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
public class JobManagementOperations {
    private final JobRunrAdminRepository jobRunrAdminRepository;
    private final StorageProvider storageProvider;

    /**
     * Método principal para actualizar estado y fecha de finalización
     * newStatus permitidos:
     * ENQUEUED
     * SCHEDULED
     * PROCESSING
     * SUCCEEDED
     * FAILED
     * DELETED
     */
    private boolean updateJobStatus(UUID jobId, String newStatus) {
        try {
            log.info("Actualizando job " + jobId + " a estado: " + newStatus);

            // 1. Obtener el job existente
            Job job = getById(jobId);

            if (job == null) {
                log.error("Job no encontrado: " + jobId);
                return false;
            }

            jobRunrAdminRepository.updateJobState(job.getId(), newStatus);

            log.info("Job actualizado exitosamente");
            return true;

        } catch (Exception e) {
            log.error("Error actualizando job " + jobId + ": " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }


    /**
     * Métodos de conveniencia
     */
    public boolean completeSuccessJob(Job job, JobResult jobResult) {
        // 1. Obtener el job existente
        if (job == null) {
            log.error("Job no encontrado");
            return false;
        }
        job = job.succeeded();
        job.getMetadata().put("finalizado", "De forma exitosa." + jobResult.getMessage());
        job.getMetadata().put("duración",jobResult.getDurationMs());
        job.getMetadata().put("momento de iniciar",jobResult.getStartedAt());
        job.getMetadata().put("momento de finalización",jobResult.getCompletedAt());
        jobRunrAdminRepository.updateJobState(job.getId(), StateName.SUCCEEDED.name());
        return true;
    }

    public boolean failJob(Job job, JobResult jobResult) {
        if (job == null) {
            log.error("Job no encontrado");
            return false;
        }
        job = job.failed(jobResult.getMessage(), new Exception("Error: " + jobResult.getMessage()
                + ". Detalles: " + jobResult.getErrorDetails()));
        job.getMetadata().put("finalizado", "Con errores: " + jobResult.getMessage());
        job.getMetadata().put("errorDetails", jobResult.getErrorDetails());
        job.getMetadata().put("duración",jobResult.getDurationMs());
        job.getMetadata().put("momento de iniciar",jobResult.getStartedAt());
        job.getMetadata().put("momento de finalización",jobResult.getCompletedAt());

        jobRunrAdminRepository.updateJobState(job.getId(), StateName.FAILED.name());
        return true;
    }

    public boolean startOrContinueJob(UUID jobId) {
        return updateJobStatus(jobId, StateName.PROCESSING.toString());
    }


    public boolean cancelJob(UUID jobId) {
        return updateJobStatus(jobId, StateName.DELETED.toString());
    }

    public Job getById(UUID jobId) {
        return storageProvider.getJobById(jobId);
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

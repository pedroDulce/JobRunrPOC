package com.company.batchscheduler.service;

import com.company.batchscheduler.repository.JobRunerRepository;
import common.batch.dto.JobResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
@Transactional
public class JobManagementOperations {

    private final StorageProvider storageProvider;
    private final JobRunerRepository jobRunerRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW, timeout = 10)
    public boolean updateJobStatus(String jobId, String state, Integer progress) {
        UUID uid = UUID.fromString(jobId);
        Job job = storageProvider.getJobById(uid);
        // Opción A: Usar metadata del job
        job.getMetadata().put("progress", progress);
        job.getMetadata().put("lastHeartbeat", Instant.now());

        jobRunerRepository.updateJobToProcessing(uid, state);

        // 3. Guardar
        storageProvider.save(job);

        return true;
    }

    @Transactional
    public boolean completeSuccessJob(Job job, JobResult jobResult) {
        if (job == null) {
            log.error("Job no encontrado");
            return false;
        }

        // 2. Añadir metadata (esto sí es mutable)
        job.getMetadata().put("finalizado", "De forma exitosa. " + jobResult.getMessage());
        job.getMetadata().put("duracionMs", String.valueOf(jobResult.getDurationMs()));
        job.getMetadata().put("inicio", jobResult.getStartedAt().toString());
        job.getMetadata().put("fin", jobResult.getCompletedAt().toString());

        // 3. Persistir el Job completo
        storageProvider.save(job);

        //updateJobStatus(job.getId().toString(), StateName.SUCCEEDED.name(), 100);

        Job succeededJob = job.succeeded();

        // 6. Guardar el job con nuevo estado
        storageProvider.save(succeededJob);

        return true;
    }


    @Transactional
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

        updateJobStatus(job.getId().toString(), StateName.FAILED.name(), 100);
        // 3. Persistir el Job completo
        storageProvider.save(failedJob);

        return true;
    }

    @Transactional
    public boolean startOrContinueJob(UUID jobId) {
        return updateJobStatus(jobId.toString(), "PROCESSING",50);
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



}

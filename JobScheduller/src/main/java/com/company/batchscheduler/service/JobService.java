package com.company.batchscheduler.service;

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

@Service
@RequiredArgsConstructor
@Slf4j
public class JobService {

    private final JobScheduler jobScheduler;
    private final StorageProvider storageProvider;

    /**
     * Eliminar un job por su ID
     */
    public boolean deleteJob(String jobId) {
        try {
            jobScheduler.delete(UUID.fromString(jobId));
            log.info("Job {} deleted successfully", jobId);
            return true;
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
     * Helper para convertir Job a Map
     */
    private Map<String, Object> convertJobToMap(Job job) {
        Map<String, Object> jobMap = new HashMap<>();
        JobDetails details = job.getJobDetails();

        jobMap.put("id", job.getId().toString());
        jobMap.put("jobName", details.getClassName());
        jobMap.put("className", details.getClassName());
        jobMap.put("methodName", details.getMethodName());
        jobMap.put("state", job.getState().name());
        jobMap.put("createdAt", job.getCreatedAt());
        jobMap.put("updatedAt", job.getUpdatedAt());
        //jobMap.put("labels", details.getLabels());

        // Información específica por estado
        switch (job.getState()) {
            case SCHEDULED:
                jobMap.put("scheduledAt", ((ScheduledState) job.getJobState()).getScheduledAt());
                break;
            case PROCESSING:
                jobMap.put("startedAt", job.getUpdatedAt());
                break;
            case SUCCEEDED:
                jobMap.put("completedAt", job.getUpdatedAt());
                break;
            case FAILED:
                jobMap.put("failedAt", job.getUpdatedAt());
                jobMap.put("errorMessage", job.getJobState().getName());
                break;
        }

        return jobMap;
    }


    /**
     * Eliminar un job recurrente por nombre
     */
    public boolean deleteRecurringJobByName(String jobName) {
        try {
            jobScheduler.deleteRecurringJob(jobName);
            log.info("Recurring job {} deleted", jobName);
            return true;
        } catch (Exception e) {
            log.error("Error deleting recurring job {}: {}", jobName, e.getMessage());
            return false;
        }
    }


}

package com.company.batchscheduler.service;

import com.company.batchscheduler.model.JobExecutorTracking;
import com.company.batchscheduler.repository.JobExecutorTrackingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobId;
import org.jobrunr.storage.StorageProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobTrackingService {

    private final JobExecutorTrackingRepository trackingRepository;
    private final StorageProvider storageProvider;

    /**
     * Actualizar tracking desde resultado del executor
     */
    @Transactional
    public void updateFromExecutorResult(String jobrunrJobId,
                                         String executorJobId,
                                         String correlationId,
                                         String status,
                                         String message,
                                         String errorDetails,
                                         LocalDateTime startedAt,
                                         LocalDateTime completedAt) {

        try {
            Optional<JobExecutorTracking> trackingOpt = trackingRepository
                    .findByJobrunrJobId(jobrunrJobId);

            JobExecutorTracking tracking;

            if (trackingOpt.isPresent()) {
                tracking = trackingOpt.get();
                tracking.setExecutorJobId(executorJobId);
                tracking.setCorrelationId(correlationId);
            } else {
                // Crear nuevo tracking
                tracking = JobExecutorTracking.builder()
                        .jobrunrJobId(jobrunrJobId)
                        .executorJobId(executorJobId)
                        .correlationId(correlationId)
                        .createdAt(LocalDateTime.now())
                        .metadata(new HashMap<>())
                        .build();
            }

            // Actualizar estado
            tracking.setExecutorStatus(status);
            tracking.setMessage(message);
            tracking.setErrorDetails(errorDetails);
            tracking.setUpdatedAt(LocalDateTime.now());

            if (startedAt != null) {
                tracking.getMetadata().put("startedAt", startedAt);
            }

            if (completedAt != null &&
                    ("COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status))) {
                tracking.setCompletedAt(completedAt);
                tracking.getMetadata().put("completedAt", completedAt);
            }

            // Obtener estado actual de JobRunr
            updateJobRunrState(tracking, jobrunrJobId);

            // Calcular duración si tenemos ambos tiempos
            if (startedAt != null && completedAt != null) {
                long durationMs = java.time.Duration.between(startedAt, completedAt).toMillis();
                tracking.getMetadata().put("durationMs", durationMs);
            }

            trackingRepository.save(tracking);

            log.info("📊 Tracking updated - JobRunr: {}, Executor: {}, Status: {}",
                    jobrunrJobId, executorJobId, status);

        } catch (Exception e) {
            log.error("Failed to update tracking for {}: {}", jobrunrJobId, e.getMessage(), e);
            throw new RuntimeException("Tracking update failed", e);
        }
    }

    /**
     * Actualizar estado de JobRunr
     */
    private void updateJobRunrState(JobExecutorTracking tracking, String jobrunrJobId) {
        try {
            UUID uuid = UUID.fromString(jobrunrJobId);
            JobId jobId = new JobId(uuid);

            Job job = storageProvider.getJobById(jobId);
            if (job != null) {
                UUID jobUuid = job.getId(); // UUID del job
                org.jobrunr.jobs.states.StateName state = job.getState();

                tracking.setJobrunrState(state.name());
                tracking.getMetadata().put("jobrunrJobUuid", jobUuid.toString());
                tracking.getMetadata().put("jobrunrJobName", job.getJobName());
                tracking.getMetadata().put("jobrunrCreatedAt", job.getCreatedAt());
                tracking.getMetadata().put("jobrunrUpdatedAt", job.getUpdatedAt());

            } else {
                tracking.setJobrunrState("NOT_FOUND");
                log.warn("JobRunr job {} not found", jobrunrJobId);
            }

        } catch (Exception e) {
            log.warn("Could not get JobRunr state for {}: {}", jobrunrJobId, e.getMessage());
            tracking.setJobrunrState("ERROR");
            tracking.getMetadata().put("jobrunrError", e.getMessage());
        }
    }

    /**
     * Obtener estado combinado
     */
    public Map<String, Object> getCombinedStatus(String jobrunrJobId) {
        Optional<JobExecutorTracking> trackingOpt = trackingRepository
                .findByJobrunrJobId(jobrunrJobId);

        if (trackingOpt.isEmpty()) {
            return Map.of(
                    "error", "Tracking not found",
                    "jobrunrJobId", jobrunrJobId
            );
        }

        JobExecutorTracking tracking = trackingOpt.get();

        Map<String, Object> result = new HashMap<>();
        result.put("jobrunrJobId", tracking.getJobrunrJobId());
        result.put("executorJobId", tracking.getExecutorJobId());
        result.put("correlationId", tracking.getCorrelationId());
        result.put("executorStatus", tracking.getExecutorStatus());
        result.put("jobrunrState", tracking.getJobrunrState());
        result.put("message", tracking.getMessage());
        result.put("combinedStatus", calculateCombinedStatus(
                tracking.getExecutorStatus(),
                tracking.getJobrunrState()
        ));
        result.put("createdAt", tracking.getCreatedAt());
        result.put("updatedAt", tracking.getUpdatedAt());
        result.put("completedAt", tracking.getCompletedAt());
        result.put("metadata", tracking.getMetadata());

        return result;
    }

    /**
     * Calcular estado combinado
     */
    private String calculateCombinedStatus(String executorStatus, String jobrunrState) {
        if ("COMPLETED".equals(executorStatus) && "SUCCEEDED".equals(jobrunrState)) {
            return "SUCCESS";
        } else if ("FAILED".equals(executorStatus) || "FAILED".equals(jobrunrState)) {
            return "FAILURE";
        } else if ("IN_PROGRESS".equals(executorStatus) || "PROCESSING".equals(jobrunrState)) {
            return "IN_PROGRESS";
        } else if ("SCHEDULED".equals(executorStatus) || "ENQUEUED".equals(jobrunrState)) {
            return "SCHEDULED";
        } else if ("CANCELLED".equals(executorStatus)) {
            return "CANCELLED";
        } else {
            return "UNKNOWN";
        }
    }

    /**
     * Crear tracking inicial cuando se programa un job
     */
    @Transactional
    public void createInitialTracking(String jobrunrJobId, String executorJobId,
                                      String correlationId, String businessDomain,
                                      String targetBatch) {

        try {
            JobExecutorTracking tracking = JobExecutorTracking.builder()
                    .jobrunrJobId(jobrunrJobId)
                    .executorJobId(executorJobId)
                    .correlationId(correlationId)
                    .executorStatus("SCHEDULED")
                    .createdAt(LocalDateTime.now())
                    .updatedAt(LocalDateTime.now())
                    .metadata(new HashMap<>())
                    .build();

            tracking.getMetadata().put("businessDomain", businessDomain);
            tracking.getMetadata().put("targetBatch", targetBatch);
            tracking.getMetadata().put("scheduledAt", LocalDateTime.now());

            trackingRepository.save(tracking);

            log.info("📝 Initial tracking created for {} -> {}", jobrunrJobId, executorJobId);

        } catch (Exception e) {
            log.error("Failed to create initial tracking: {}", e.getMessage());
        }
    }
}

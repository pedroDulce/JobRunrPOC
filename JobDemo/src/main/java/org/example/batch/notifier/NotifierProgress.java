package org.example.batch.notifier;

import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotifierProgress {

    private final KafkaPublisher kafkaPublisher;

    /**
     * Notifica progreso del batch job
     */
    public void notifyProgress(String jobId, String message, int progress) {

        JobResult statusResult = JobResult.builder()
                .jobId(jobId)
                .jobName("jobRequest.getJobName()")
                .status(JobStatusEnum.IN_PROGRESS)
                .message(message)
                .startedAt(LocalDateTime.now())
                .completedAt(null)
                .errorDetails(null)
                .correlationId("correlationId")
                .jobrunrJobId(jobId)  // IMPORTANTE: ID de JobRunr
                .build();

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        kafkaPublisher.publishToResultsTopic(statusResult);

        log.debug("📤 Notificado PROGRESO del batch job {}: {}%", jobId, progress);
    }

    /**
     * Notifica inicio del batch job
     */
    public void notifyStart(String jobId, String message) {

        JobResult statusResult = JobResult.builder()
                .jobId(jobId)
                .jobName("jobRequest.getJobName()")
                .status(JobStatusEnum.IN_PROGRESS)
                .message(message)
                .startedAt(LocalDateTime.now())
                .completedAt(null)
                .errorDetails(null)
                .correlationId("correlationId")
                .jobrunrJobId(jobId)  // IMPORTANTE: ID de JobRunr
                .build();

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        kafkaPublisher.publishToResultsTopic(statusResult);

        log.info("📤 JobExecutor: Published final result for job {} with status {}",
                statusResult.getJobId(), JobStatusEnum.IN_PROGRESS);

        log.info("📤 Notificado INICIO del batch job: {}", jobId);
    }

    /**
     * Notifica finalización del batch job
     */
    public void notifyCompletion(String jobId, String status, String message,
                                 Map<String, Object> report) {

        JobResult statusResult = JobResult.builder()
                .jobId(jobId)
                .jobName("jobRequest.getJobName()")
                .status(JobStatusEnum.COMPLETED)
                .message(message)
                .startedAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .errorDetails(null)
                .correlationId("correlationId")
                .metadata(report != null ? report : Map.of("stage", "COMPLETED"))
                .jobrunrJobId(jobId)  // IMPORTANTE: ID de JobRunr
                .build();

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        kafkaPublisher.publishToResultsTopic(statusResult);

        log.info("📤 Notificado COMPLETADO del batch job {}: {}", jobId, status);
    }


}

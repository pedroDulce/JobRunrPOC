package com.ad.muface.batch.notifier;

import com.ad.muface.batch.dto.JobResult;
import com.ad.muface.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaPublisher {

    @Value("${kafka.topics.job-results}")
    private String jobResultsTopic;

    private final KafkaTemplate<String, JobResult> kafkaTemplate;

    public void notifyProgress(String jobId, String jobname, String correlationId,
                               String message, JobResult statusResult) {

        statusResult.setJobName(jobname);
        statusResult.setMessage(message);
        statusResult.setCorrelationId(correlationId);

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        this.publishToResultsTopic(statusResult);

        log.debug("📤 Notificado PROGRESO del batch job {}", jobId, jobname);
    }

    /**
     * Notifica inicio del batch job
     */
    public void notifyStart(String jobId, String jobname, String correlationId, String message, JobExecution jobExecution) {

        JobResult statusResult = JobResult.builder()
                .jobId(jobId)
                .jobName(jobname)
                .status(JobStatusEnum.IN_PROGRESS)
                .message(message)
                .startedAt(jobExecution.getStartTime())
                .lastHeartBeat(jobExecution.getLastUpdated())
                .completedAt(null)
                .errorDetails(null)
                .correlationId(correlationId)
                .jobrunrJobId(jobId)
                .build();

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        this.publishToResultsTopic(statusResult);

        log.info("📤 JobExecutor: Published final result for job {} with status {}",
                statusResult.getJobId(), JobStatusEnum.IN_PROGRESS);

        log.info("📤 Notificado INICIO del batch job: {}  {}", jobId, jobname);
    }


    public void notifyFailure(String jobId, String jobname, String correlationId,
                              String message, JobExecution jobExecution) {

        long durationSeconds = 0L;
        if (jobExecution != null) {
            durationSeconds = Duration.between(
                    jobExecution.getStartTime(),
                    jobExecution.getEndTime()
            ).getSeconds()*1000;
        }

        JobResult statusResult = JobResult.builder()
                .jobId(jobId)
                .jobName(jobname)
                .status(JobStatusEnum.FAILED)
                .message(message)
                .completedAt(LocalDateTime.now())
                .executionTimeInMills(durationSeconds)
                .errorDetails(message)
                .correlationId(correlationId)
                .jobrunrJobId(jobId)
                .build();

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        this.publishToResultsTopic(statusResult);

        log.info("📤 Notificado FAILED batch, job {}: {}", jobId, jobname);
    }

    public void notifyCompletion(String jobId, String jobname, String correlationId, String message,
                                 Map<String, Object> report, JobExecution jobExecution) {

        long durationSeconds = Duration.between(
                jobExecution.getStartTime(),
                jobExecution.getEndTime()
        ).getSeconds()*1000;

        JobResult statusResult = JobResult.builder()
                .jobId(jobId)
                .jobName(jobname)
                .status(JobStatusEnum.COMPLETED)
                .message(message)
                .completedAt(LocalDateTime.now())
                .executionTimeInMills(durationSeconds)
                .errorDetails(null)
                .correlationId(correlationId)
                .metadata(report != null ? report : Map.of("stage", "COMPLETED"))
                .jobrunrJobId(jobId)  // IMPORTANTE: ID de JobRunr
                .build();

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        this.publishToResultsTopic(statusResult);

        log.info("📤 Notificado COMPLETADO del batch job {}: {}", jobId, jobname);
    }

    /**
     * Publicar al topic de resultados
     */
    private void publishToResultsTopic(JobResult result) {
        String key = result.getJobId();

        CompletableFuture<SendResult<String, JobResult>> future =
                kafkaTemplate.send(jobResultsTopic, key, result);

        future.whenComplete((sendResult, throwable) -> {
            if (throwable != null) {
                log.error("JobExecutor: Failed to publish to {} for job {}: {}",
                        jobResultsTopic, key, throwable.getMessage());
            } else {
                log.debug("JobExecutor: Published to {} for job {}: partition {}, offset {}",
                        jobResultsTopic, key,
                        sendResult.getRecordMetadata().partition(),
                        sendResult.getRecordMetadata().offset());
            }
        });
    }


}

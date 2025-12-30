package org.example.batch.notifier;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaPublisher {

    @Value("${kafka.topics.job-results}")
    private String jobResultsTopic;

    private final KafkaTemplate<String, JobResult> kafkaTemplate;

    /**
     * Publicar resultado final
     */
    public void publishJobResult(JobResult result,
                                 String correlationId,
                                 String jobrunrJobId) {

        // Asegurar que tiene el jobrunrJobId
        if (jobrunrJobId != null) {
            result.setJobrunrJobId(jobrunrJobId);
        }
        if (correlationId != null) {
            result.setCorrelationId(correlationId);
        }

        publishToResultsTopic(result);

        log.info("📤 JobExecutor: Published final result for job {} with status {}",
                result.getJobId(), result.getStatus());
    }

    /**
     * Publicar estado del job al scheduler
     */
    public void publishJobStatus(JobRequest jobRequest,
                                 JobStatusEnum status,
                                 Exception error,
                                 String correlationId,
                                 String jobrunrJobId,
                                 String message) {

        try {
            JobResult statusResult = JobResult.builder()
                    .jobId(jobRequest.getJobId())
                    .jobName(jobRequest.getJobName())
                    .status(status)
                    .message(message)
                    .startedAt(LocalDateTime.now())
                    .completedAt(status.compareTo(JobStatusEnum.COMPLETED) == 0 || status.compareTo(JobStatusEnum.FAILED) == 0
                            ? LocalDateTime.now() : null)
                    .errorDetails(error != null ? error.getMessage() : null)
                    .correlationId(correlationId)
                    .jobrunrJobId(jobrunrJobId)  // IMPORTANTE: ID de JobRunr
                    .build();

            publishToResultsTopic(statusResult);

            log.debug("📤 JobExecutor: Published job status: {} for job {}", status, jobRequest.getJobId());

        } catch (Exception e) {
            log.error("JobExecutor: Failed to publish job status for {}: {}",
                    jobRequest.getJobId(), e.getMessage());
        }
    }


    /**
     * Publicar al topic de resultados
     */
    public void publishToResultsTopic(JobResult result) {
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

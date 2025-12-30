package org.example.batch.notifier;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.batch.job.CustomerSummaryJob;
import org.example.batch.service.BatchJobExecutorService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobNotifier {

    @Value("${kafka.topics.job-results}")
    private String jobResultsTopic;

    private final CustomerSummaryJob jobExecutionService;
    private final BatchJobExecutorService batchJobExecutorService;
    private final KafkaTemplate<String, JobResult> kafkaTemplate;

    @KafkaListener(
            topics = "${kafka.topics.job-requests}",
            containerFactory = "jobRequestListenerContainerFactory",
            groupId = "${spring.kafka.consumer.group-id}",
            id = "job-request-consumer"
    )
    @Transactional
    public void consumeJobRequest(
            ConsumerRecord<String, JobRequest> record,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = "business-domain", required = true) String businessDomain,
            @Header(value = "target-job", required = true) String targetJob,
            @Header(value = "priority", defaultValue = "MEDIUM") String priority,
            @Header(value = "correlation-id", required = true) String correlationId,
            @Header(value = "jobrunr-job-id", required = true) String jobrunrJobId,
            Acknowledgment acknowledgment) {

        JobResult result;
        try {
            JobRequest jobRequest = record.value();

            log.info("""
                    📥 JobExecutor: Received Job Request:
                    Job ID: {}
                    JobRunr Job ID: {}
                    Business Domain: {}
                    Target Batch: {}
                    Priority: {}
                    Correlation ID: {}
                    """,
                    jobRequest.getJobId(),
                    jobrunrJobId,
                    businessDomain,
                    targetJob,
                    priority,
                    correlationId
            );

            // 1. Publicar estado IN_PROGRESS
            publishJobStatus(jobRequest, JobStatusEnum.IN_PROGRESS, null,
                    correlationId, jobrunrJobId, "JobExecutor: remote Job execution started");

            // 2. Ejecutar el job
            result = jobExecutionService.executeJob(jobRequest, extractHeaders(record));

            // 3. Publicar resultado final
            publishJobResult(result, correlationId, jobrunrJobId);

            // 4. Confirmar offset
            acknowledgment.acknowledge();

            log.info("✅ JobExecutor: Job {} executed successfully", jobRequest.getJobId());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing job request: {}", e.getMessage(), e);

            // Publicar estado FAILED si hay jobRequest
            if (record != null && record.value() != null) {
                JobRequest jobRequest = record.value();
                publishJobStatus(jobRequest, JobStatusEnum.FAILED, e,
                        correlationId, jobrunrJobId, "Job execution failed: " + e.getMessage());
            }

            // No confirmar para que se reintente
        }
    }

    @Transactional
    public void consumeBatchRequest(
            ConsumerRecord<String, JobRequest> record,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = "business-domain", required = true) String businessDomain,
            @Header(value = "target-batch", required = true) String targetBatch,
            @Header(value = "priority", defaultValue = "MEDIUM") String priority,
            @Header(value = "correlation-id", required = true) String correlationId,
            @Header(value = "jobrunr-job-id", required = true) String jobrunrJobId,
            @Payload Map<String, Object> payload,
            Acknowledgment acknowledgment) {

        JobResult result;
        try {
            JobRequest jobRequest = record.value();

            log.info("""
                    📥 JobExecutor: Received Batch Request:
                    Job ID: {}
                    JobRunr Job ID: {}
                    Business Domain: {}
                    Target Batch: {}
                    Priority: {}
                    Correlation ID: {}
                    """,
                    jobRequest.getJobId(),
                    jobrunrJobId,
                    businessDomain,
                    targetBatch,
                    priority,
                    correlationId
            );

            // 1. Publicar estado IN_PROGRESS
            publishJobStatus(jobRequest, JobStatusEnum.IN_PROGRESS, null,
                    correlationId, jobrunrJobId, "JobExecutor: remote Batch execution started");

            // 2. Ejecutar el batch
            batchJobExecutorService.executeSpringBatchJob(jobrunrJobId, jobRequest.getJobName(),
                    jobRequest.getParameters());

            // 4. Confirmar offset
            acknowledgment.acknowledge();

            log.info("✅ JobExecutor: Batch {} executed successfully", jobRequest.getJobId());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing Batch request: {}", e.getMessage(), e);

            // Publicar estado FAILED si hay jobRequest
            if (record != null && record.value() != null) {
                JobRequest jobRequest = record.value();
                publishJobStatus(jobRequest, JobStatusEnum.FAILED, e,
                        correlationId, jobrunrJobId, "Batch execution failed: " + e.getMessage());
            }

            // No confirmar para que se reintente
        }
    }


    /**
     * Publicar estado del job al scheduler
     */
    private void publishJobStatus(JobRequest jobRequest,
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
     * Publicar resultado final
     */
    private void publishJobResult(JobResult result,
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

    /**
     * Extraer headers
     */
    private Map<String, String> extractHeaders(ConsumerRecord<String, JobRequest> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> {
            headers.put(header.key(), new String(header.value()));
        });
        return headers;
    }
}

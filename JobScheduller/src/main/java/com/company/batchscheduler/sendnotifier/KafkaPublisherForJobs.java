package com.company.batchscheduler.sendnotifier;

import com.company.batchscheduler.service.JobService;
import common.batch.dto.JobRequest;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.annotations.Job;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.jobrunr.jobs.context.JobContext;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
@Slf4j
@Component
public class KafkaPublisherForJobs {

    @Value("${kafka.topics.job-requests}")
    private String jobRequestsTopic;

    private final JobService jobService;
    private final KafkaTemplate<String, JobRequest> kafkaTemplate;

    /**
     * Publica un evento de job con headers de routing para filtrado
     */
    @Job(name = "Ejecutar job en microservicio de forma remota con invocación asíncrona")
    public JobStatusEnum publishEventForRunJob(JobRequest request, JobContext jobContext) {

        UUID jobExecutionId = jobContext.getJobId();

        request.setScheduledAt(LocalDateTime.now());

        try {
            // Crear mensaje con headers de routing
            Message<JobRequest> message = buildMessageWithRoutingHeaders(jobExecutionId, request);

            // Publicar a Kafka
            CompletableFuture<SendResult<String, JobRequest>> future = kafkaTemplate.send(message);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    handlePublishFailure(jobExecutionId, ex);
                } else {
                    handlePublishSuccess(jobExecutionId, result);
                }
            });
            jobService.startJob(jobExecutionId);

            return JobStatusEnum.IN_PROGRESS;

        } catch (Exception e) {
            log.error("Error sending to Kafka: {}", e.getMessage());
            return JobStatusEnum.FAILED;
        }
    }

    /**
     * Construye mensaje con headers de routing para filtrado
     */
    private Message<JobRequest> buildMessageWithRoutingHeaders(UUID jobExecutionId, JobRequest request) {
        String correlationId = generateCorrelationId();
        String jobRunrJobId = jobExecutionId.toString();

        log.info("🎯 JobRunr Job created - For Executor Job with ID: {}", jobRunrJobId);

        return MessageBuilder
                .withPayload(request)
                // Headers principales para routing
                .setHeader(KafkaHeaders.TOPIC, jobRequestsTopic)
                .setHeader(KafkaHeaders.KEY, jobExecutionId)
                .setHeader("job-id", jobExecutionId)
                .setHeader("jobrunr-job-id", jobRunrJobId)
                // Headers de routing/filtrado
                .setHeader("job-type", request.getJobType())          // "ASYNCRONOUS"
                .setHeader("business-domain", request.getBusinessDomain()) // Ej: "application-job-demo"
                .setHeader("target-batch", request.getJobName()) // Ej: "ResumenDiarioClientesAsync"

                // Headers de procesamiento
                .setHeader("priority", request.getPriority())         // Ej: "HIGH", "MEDIUM", "LOW"
                .setHeader("retry-count", 0)
                .setHeader("scheduled-at", request.getScheduledAt())
                .setHeader("time-to-live", request.getTtl())

                // Headers técnicos
                .setHeader("source", "batch-scheduler-service")
                .setHeader("version", "1.0")
                .setHeader("correlation-id", correlationId)
                .setHeader("producer-timestamp", System.currentTimeMillis())
                .setHeader("event-created-at", LocalDateTime.now().toString())
                .setHeader("scheduled-at", request.getScheduledAt() != null ?
                        request.getScheduledAt().toString() : LocalDateTime.now().toString())
                .build();
    }

    public void trackJobInJobRunr(String executorJobId, String correlationId) {
        log.trace("JobRunr tracking - Executor Job ID: {}, Correlation: {}",
                executorJobId, correlationId);
    }

    /**
     * Maneja éxito en publicación
     */
    private void handlePublishSuccess(UUID jobId, SendResult<String, JobRequest> result) {
        log.info("""
                Job {} published to Kafka successfully.
                Topic: {}
                Partition: {}
                Offset: {}
                Headers: {}
                """,
                jobId,
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset(),
                result.getProducerRecord().headers()
        );
    }

    /**
     * Maneja fallo en publicación
     */
    private void handlePublishFailure(UUID jobId, Throwable ex) {
        log.error("Failed to publish job {} to Kafka: {}", jobId, ex.getMessage());
    }

    /**
     * Genera correlation ID para tracing
     */
    private String generateCorrelationId() {
        return "corr-" + System.currentTimeMillis() + "-" +
                java.util.UUID.randomUUID().toString().substring(0, 8);
    }


}

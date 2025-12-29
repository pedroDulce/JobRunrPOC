package com.company.batchscheduler.producer;

import com.company.batchscheduler.model.JobStatus;
import com.company.batchscheduler.service.JobStatusService;
import common.batch.dto.JobRequest;
import common.batch.dto.JobStatusEnum;
import common.batch.dto.JobType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
@Slf4j
@Component
public class KafkaPublisherForJobs {

    @Value("${kafka.topics.job-requests}")
    private String jobRequestsTopic;

    private final JobStatusService jobStatusService;
    private final KafkaTemplate<String, JobRequest> kafkaTemplate;

    /**
     * Publica un evento de job con headers de routing para filtrado
     */
    public JobStatus publishEventForRunJob(String jobId, JobRequest request) {
        request.setScheduledAt(LocalDateTime.now());
        // Guardar estado inicial en BD
        JobStatus status = JobStatus.builder()
                .jobId(jobId)
                .jobType(JobType.ASYNCRONOUS.toString())
                .status(JobStatusEnum.ENQUEUED.toString())
                .message("Job enqueued for execution")
                .createdAt(LocalDateTime.now())
                .metadata(request.getMetadata())
                .build();
        jobStatusService.saveOrUpdate(status);

        try {
            // Crear mensaje con headers de routing
            Message<JobRequest> message = buildMessageWithRoutingHeaders(jobId, request);

            // Publicar a Kafka
            CompletableFuture<SendResult<String, JobRequest>> future =
                    kafkaTemplate.send(message);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    handlePublishFailure(jobId, ex);
                } else {
                    handlePublishSuccess(jobId, result);
                }
            });

        } catch (Exception e) {
            log.error("Error sending to Kafka: {}", e.getMessage());
            status.setStatus(JobStatusEnum.FAILED.toString());
            status.setMessage("Failed to enqueue job: " + e.getMessage());
            jobStatusService.saveOrUpdate(status);
        }
        return status;
    }

    /**
     * Construye mensaje con headers de routing para filtrado
     */
    private Message<JobRequest> buildMessageWithRoutingHeaders(String jobId, JobRequest request) {
        return MessageBuilder
                .withPayload(request)
                // Headers principales para routing
                .setHeader(KafkaHeaders.TOPIC, jobRequestsTopic)
                .setHeader(KafkaHeaders.KEY, jobId)
                .setHeader("job-id", jobId)

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
                .setHeader("correlation-id", generateCorrelationId())
                .setHeader("producer-timestamp", System.currentTimeMillis())
                .setHeader("event-created-at", LocalDateTime.now().toString())
                .setHeader("scheduled-at", request.getScheduledAt() != null ?
                        request.getScheduledAt().toString() : LocalDateTime.now().toString())
                .build();
    }

    /**
     * Maneja éxito en publicación
     */
    private void handlePublishSuccess(String jobId, SendResult<String, JobRequest> result) {
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

        // Actualizar estado con información de Kafka
        updateJobStatus(jobId, JobStatusEnum.PUBLISHED.toString(),
                String.format("Published to Kafka. Offset: %d, Partition: %d",
                        result.getRecordMetadata().offset(),
                        result.getRecordMetadata().partition()));
    }

    /**
     * Maneja fallo en publicación
     */
    private void handlePublishFailure(String jobId, Throwable ex) {
        log.error("Failed to publish job {} to Kafka: {}", jobId, ex.getMessage());

        updateJobStatus(jobId, JobStatusEnum.FAILED.toString(),
                String.format("Failed to publish to Kafka: %s", ex.getMessage()));
    }

    /**
     * Genera correlation ID para tracing
     */
    private String generateCorrelationId() {
        return "corr-" + System.currentTimeMillis() + "-" +
                java.util.UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Método helper para actualizar estado de forma asíncrona
     */
    @Async
    public void updateJobStatus(String jobId, String status, String message) {
        jobStatusService.findByJobId(jobId).ifPresent(jobStatus -> {
            jobStatus.setStatus(status);
            jobStatus.setMessage(message);
            jobStatus.setUpdatedAt(LocalDateTime.now());

            // Si es estado final, registrar timestamp de finalización
            if (JobStatusEnum.COMPLETED.toString().equals(status) || JobStatusEnum.FAILED.toString().equals(status)
                    || JobStatusEnum.CANCELLED.toString().equals(status)) {
                jobStatus.setCompletedAt(LocalDateTime.now());
            }

            jobStatusService.saveOrUpdate(jobStatus);
            log.debug("Job {} status updated to: {} - {}", jobId, status, message);
        });
    }

}

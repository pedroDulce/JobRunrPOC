package com.company.batchscheduler.sendnotifier;

import common.batch.dto.JobRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaPublisherForJobs {

    @Value("${kafka.topics.job-requests}")
    private String jobRequestsTopic;

    private final KafkaTemplate<String, JobRequest> kafkaTemplate;

    /**
     * Publica job recurrente cuando JobRunr lo dispara
     * ⚠️ Este método es llamado por JobRunr CADA VEZ que se ejecuta el cron
     */
    public void publishRecurringJobEvent(JobRequest request, String correlationId) {
        log.info("🔁 Recurring job triggered: {}, Correlation: {}",
                request.getJobId(), correlationId);

        // El job YA ESTÁ creado en JobRunr como recurrente
        // Solo publicamos el mensaje en Kafka
        publishToKafka(request, correlationId, "RECURRING", null);
    }

    /**
     * Publica job programado cuando JobRunr lo dispara
     */
    public void publishScheduledJobEvent(JobRequest request, String correlationId) {
        log.info("⏰ Scheduled job triggered: {}, Correlation: {}",
                request.getJobId(), correlationId);

        publishToKafka(request, correlationId, "SCHEDULED", request.getScheduledAt());
    }

    /**
     * Publica job inmediato
     */
    public void publishImmediateJobEvent(JobRequest request, String correlationId) {
        log.info("🚀 Immediate job execution: {}, Correlation: {}",
                request.getJobId(), correlationId);

        publishToKafka(request, correlationId, "IMMEDIATE", LocalDateTime.now());
    }

    /**
     * Método central para publicar en Kafka
     */
    private void publishToKafka(JobRequest request, String correlationId,
                                String executionType, LocalDateTime scheduledAt) {

        try {
            Message<JobRequest> message = buildKafkaMessage(
                    request, correlationId, executionType, scheduledAt
            );

            CompletableFuture<SendResult<String, JobRequest>> future =
                    kafkaTemplate.send(message);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    handlePublishFailure(request.getJobId(), correlationId, ex);
                } else {
                    handlePublishSuccess(request.getJobId(), correlationId, result);
                }
            });

        } catch (Exception e) {
            log.error("❌ Failed to publish job {} to Kafka: {}",
                    request.getJobId(), e.getMessage(), e);
            throw new RuntimeException("Kafka publish failed", e);
        }
    }

    /**
     * Construye mensaje Kafka SIN jobrunr-job-id (porque no hay job individual)
     */
    private Message<JobRequest> buildKafkaMessage(JobRequest request, String correlationId,
                                                  String executionType, LocalDateTime scheduledAt) {

        return MessageBuilder
                .withPayload(request)
                .setHeader(KafkaHeaders.TOPIC, jobRequestsTopic)
                .setHeader(KafkaHeaders.KEY, request.getJobId())
                .setHeader("correlation-id", correlationId)
                .setHeader("execution-type", executionType)  // RECURRING, SCHEDULED, IMMEDIATE
                .setHeader("business-domain", request.getBusinessDomain())
                .setHeader("target-batch", request.getJobName())
                .setHeader("job-type", request.getJobType())
                .setHeader("priority", request.getPriority())
                .setHeader("retry-count", 0)
                .setHeader("scheduled-at", scheduledAt != null ?
                        scheduledAt : LocalDateTime.now())
                .setHeader("source", "batch-scheduler-service")
                .setHeader("version", "1.0")
                .setHeader("producer-timestamp", System.currentTimeMillis())
                .setHeader("event-created-at", LocalDateTime.now())
                .build();
    }

    private void handlePublishSuccess(String jobId, String correlationId,
                                      SendResult<String, JobRequest> result) {
        log.info("""
                ✅ Job published to Kafka:
                - Job: {}
                - Correlation: {}
                - Topic: {}
                - Partition: {}
                - Offset: {}
                """,
                jobId, correlationId,
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset()
        );
    }

    private void handlePublishFailure(String jobId, String correlationId, Throwable ex) {
        log.error("❌ Failed to publish job {} (Correlation: {}): {}",
                jobId, correlationId, ex.getMessage());

        // ⚠️ IMPORTANTE: No podemos marcar como FAILED en JobRunr porque:
        // 1. Para jobs recurrentes, no hay job individual para esta ejecución
        // 2. JobRunr solo tiene el job recurrente padre
        // 3. La próxima ejecución del cron intentará de nuevo

        // Podrías:
        // 1. Enviar a Dead Letter Queue
        // 2. Notificar a un administrador
        // 3. Registrar en log de errores
    }
}

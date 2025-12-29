package org.example.batch.consumer;

import common.batch.dto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.batch.job.CustomerSummaryJob;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobRequestConsumer {

    private final CustomerSummaryJob jobExecutionService;
    private final RetryTemplate retryTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ExecutorService highPriorityExecutor = Executors.newFixedThreadPool(2);
    private final ExecutorService normalPriorityExecutor = Executors.newFixedThreadPool(5);

    @KafkaListener(
            topics = "${kafka.topics.job-requests}",
            containerFactory = "jobRequestListenerContainerFactory",
            groupId = "${spring.kafka.consumer.group-id}",
            id = "job-request-consumer"
    )
    public void consumeJobRequest(
            ConsumerRecord<String, JobRequest> record,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(value = "business-domain", required = false) String businessDomain,
            @Header(value = "target-batch", required = false) String targetBatch,
            @Header(value = "job-type", required = false) String jobType,
            @Header(value = "priority", defaultValue = "MEDIUM") String priority,
            @Header(value = "correlation-id", required = false) String correlationId,
            Acknowledgment acknowledgment) {

        try {
            JobRequest jobRequest = record.value();

            log.info("""
                📥 Received Job Request (Filtered):
                Job ID: {}
                Business Domain: {}
                Target Batch: {}
                Job Name: {}
                Key: {}
                Partition: {}
                Job Type: {}
                Priority: {}
                """,
                    jobRequest.getJobId(),
                    businessDomain,
                    targetBatch,
                    jobRequest.getJobName(),
                    key,
                    partition,
                    jobType,
                    priority /*JobPriority.HIGH*/
            );

            // Ejecutar el job
            log.info("Executing job: {} for business-domain: {}",
                    jobRequest.getJobName(), businessDomain);

            processWithHighPriority(jobRequest, extractHeaders(record), correlationId);

            // Confirmar procesamiento
            acknowledgment.acknowledge();
            log.info("✅ Job {} executed successfully", jobRequest.getJobId());

        } catch (Exception e) {
            log.error("❌ Error processing job request: {}", e.getMessage(), e);
            shutdown();
        }
    }

    /**
     * Procesar job con alta prioridad
     */
    private JobResult processWithHighPriority(JobRequest jobRequest,
                                              Map<String, String> headers,
                                              String correlationId) {
        log.info("⚡ Processing HIGH priority job: {}, with correlationId: {}", jobRequest.getJobId(), correlationId);

        try {
            // Ejecutar en thread separado para no bloquear el consumer
            CompletableFuture<JobResult> future = CompletableFuture.supplyAsync(() -> {
                return jobExecutionService.executeJob(jobRequest, headers);
            }, highPriorityExecutor);

            // Timeout para alta prioridad: 30 segundos
            return future.get(30, java.util.concurrent.TimeUnit.SECONDS);

        } catch (java.util.concurrent.TimeoutException e) {
            log.error("Timeout processing high priority job: {}", jobRequest.getJobId());
            return JobResult.builder()
                    .jobId(jobRequest.getJobId())
                    .jobName(jobRequest.getJobName())
                    .status(JobStatusEnum.FAILED)
                    .message("Timeout processing high priority job")
                    .startedAt(LocalDateTime.now())
                    .completedAt(LocalDateTime.now())
                    .errorDetails("Timeout after 30 seconds")
                    .build();
        } catch (Exception e) {
            log.error("Error processing high priority job: {}", jobRequest.getJobId(), e);
            return JobResult.builder()
                    .jobId(jobRequest.getJobId())
                    .jobName(jobRequest.getJobName())
                    .status(JobStatusEnum.FAILED)
                    .message("Error processing high priority job")
                    .startedAt(LocalDateTime.now())
                    .completedAt(LocalDateTime.now())
                    .errorDetails(e.getMessage())
                    .build();
        }
    }

    /**
     * Extraer headers del record
     */
    private Map<String, String> extractHeaders(ConsumerRecord<String, JobRequest> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> {
            headers.put(header.key(), new String(header.value()));
        });
        return headers;
    }


    /**
     * Listener para reprocesar mensajes de DLQ
     */
    @KafkaListener(
            topics = "${kafka.topics.job-requests}-dlq",
            containerFactory = "jobRequestListenerContainerFactory",
            groupId = "${spring.kafka.consumer.group-id}-dlq",
            id = "dlq-consumer"
    )
    public void consumeDlqMessage(
            ConsumerRecord<String, Map<String, Object>> record,
            Acknowledgment acknowledgment) {

        log.warn("Processing DLQ message: {}", record.key());

        try {
            Map<String, Object> dlqPayload = record.value();

            // Extraer información del error
            String error = (String) dlqPayload.get("error");
            String errorType = (String) dlqPayload.get("errorType");

            log.error("DLQ Error for job {}: {} - {}", record.key(), errorType, error);

            // Aquí podrías:
            // 1. Notificar a un administrador
            // 2. Intentar reparar y reprocesar
            // 3. Guardar en base de datos para análisis

            // Por ahora, solo loggear y confirmar
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing DLQ message: {}", e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge para no bloquear
        }
    }

    /**
     * Método para limpiar recursos
     */
    public void shutdown() {
        highPriorityExecutor.shutdown();
        normalPriorityExecutor.shutdown();

        try {
            if (!highPriorityExecutor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                highPriorityExecutor.shutdownNow();
            }
            if (!normalPriorityExecutor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                normalPriorityExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            highPriorityExecutor.shutdownNow();
            normalPriorityExecutor.shutdownNow();
        }
    }

}

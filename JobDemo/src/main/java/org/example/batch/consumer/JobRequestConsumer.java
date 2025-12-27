package org.example.batch.consumer;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
            @Header(value = "job-type", required = false) String jobType,
            @Header(value = "target-service", required = false) String targetService,
            @Header(value = "priority", defaultValue = "MEDIUM") String priority,
            @Header(value = "correlation-id", required = false) String correlationId,
            Acknowledgment acknowledgment) {

        try {
            JobRequest jobRequest = record.value();

            log.info("""
                    📥 Received Job Request:
                    Job ID: {}
                    Job Name: {}
                    Key: {}
                    Partition: {}
                    Offset: {}
                    Topic: {}
                    Job Type: {}
                    Target Service: {}
                    Priority: {}
                    Correlation ID: {}
                    """,
                    jobRequest.getJobId(),
                    jobRequest.getJobName(),
                    key,
                    partition,
                    record.offset(),
                    topic,
                    jobType,
                    targetService,
                    priority,
                    correlationId
            );

            // Ejecutar el job
            log.info("Executing job: {}", jobRequest.getJobName());

            // Aquí llamarías a tu servicio de ejecución
            // jobExecutionService.executeJob(jobRequest);

            // Por ahora solo loggear
            log.info("✅ Job {} executed successfully", jobRequest.getJobId());

            // Confirmar procesamiento
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("❌ Error processing job request: {}", e.getMessage(), e);
            // No confirmar para que se reintente
        }
    }

    /**
     * Procesar job con prioridad normal
     */
    private JobResult processWithNormalPriority(JobRequest jobRequest,
                                                Map<String, String> headers,
                                                String correlationId) {
        log.debug("Processing normal priority job: {}", jobRequest.getJobId());

        try {
            // Usar RetryTemplate para reintentos automáticos
            return retryTemplate.execute(context -> {
                int retryCount = context.getRetryCount();
                if (retryCount > 0) {
                    log.warn("Retry attempt {} for job {}", retryCount, jobRequest.getJobId());
                }
                return jobExecutionService.executeJob(jobRequest, headers);
            });

        } catch (Exception e) {
            log.error("Error processing normal priority job: {}", jobRequest.getJobId(), e);
            return JobResult.builder()
                    .jobId(jobRequest.getJobId())
                    .jobName(jobRequest.getJobName())
                    .status(JobStatusEnum.FAILED)
                    .message("Error processing job after retries")
                    .startedAt(LocalDateTime.now())
                    .completedAt(LocalDateTime.now())
                    .errorDetails(e.getMessage())
                    .build();
        }
    }

    /**
     * Listener para jobs de alta prioridad (separado)
     */
    @KafkaListener(
            topics = "${kafka.topics.job-requests}",
            containerFactory = "highPriorityListenerContainerFactory",
            groupId = "${spring.kafka.consumer.group-id}-high-priority",
            id = "high-priority-consumer"
    )
    public void consumeHighPriorityJob(
            ConsumerRecord<String, JobRequest> record,
            @Header("priority") String priority,
            Acknowledgment acknowledgment) {

        if (!"HIGH".equals(priority)) {
            return;
        }

        log.info("⚡ Processing HIGH priority job from dedicated consumer: {}", record.key());

        processWithHighPriority(record.value(), extractHeaders(record), null);
        acknowledgment.acknowledge();
    }


    /**
     * Procesar job con alta prioridad
     */
    private JobResult processWithHighPriority(JobRequest jobRequest,
                                              Map<String, String> headers,
                                              String correlationId) {
        log.info("⚡ Processing HIGH priority job: {}", jobRequest.getJobId());

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
     * Verificar si el job es para este servicio
     */
    private boolean isJobForThisService(JobRequest jobRequest, String targetService) {
        // 1. Verificar header target-service
        if (targetService != null) {
            return targetService.equals(getServiceName());
        }

        // 2. Verificar por job type
        return isJobTypeSupported(jobRequest.getJobType());
    }

    private String getServiceName() {
        // Obtener del application.properties
        return "customer-service"; // Cambiar según el servicio
    }

    private boolean isJobTypeSupported(String jobType) {
        // Definir qué job types soporta este servicio
        List<String> supportedJobTypes = Arrays.asList(
                "CUSTOMER_SUMMARY",
                "CUSTOMER_EXPORT",
                "CUSTOMER_SYNC"
                // Añadir más según el servicio
        );
        return supportedJobTypes.contains(jobType);
    }

    /**
     * Manejo de errores
     */
    private void handleProcessingError(
            ConsumerRecord<String, JobRequest> record,
            Exception exception,
            Acknowledgment acknowledgment) {

        // Política de reintentos
        Integer retryCount = getRetryCount(record);

        if (retryCount < getMaxRetries()) {
            // Reintentar
            log.warn("Retrying job {} (attempt {}/{})",
                    record.key(), retryCount + 1, getMaxRetries());

            // Podrías enviar a un topic de retry
            sendToRetryTopic(record, retryCount + 1);

        } else {
            // Enviar a Dead Letter Queue
            log.error("Max retries exceeded for job {}. Sending to DLQ", record.key());
            sendToDlq(record, exception);
        }

        acknowledgment.acknowledge();
    }

    /**
     * Enviar mensaje al topic de reintento
     */
    private void sendToRetryTopic(ConsumerRecord<String, JobRequest> record, int newRetryCount) {
        try {
            // Crear nuevo mensaje con contador de reintentos actualizado
            org.springframework.messaging.Message<JobRequest> message =
                    org.springframework.messaging.support.MessageBuilder
                            .withPayload(record.value())
                            .setHeader(org.springframework.kafka.support.KafkaHeaders.TOPIC,
                                    record.topic() + "-retry")
                            .setHeader(org.springframework.kafka.support.KafkaHeaders.KEY, record.key())
                            .setHeader("original-topic", record.topic())
                            .setHeader("original-partition", record.partition())
                            .setHeader("original-offset", record.offset())
                            .setHeader("retry-count", String.valueOf(newRetryCount))
                            .setHeader("retry-timestamp", LocalDateTime.now().toString())
                            .setHeader("next-retry-delay", calculateNextRetryDelay(newRetryCount))
                            .build();

            kafkaTemplate.send(message);

            log.info("Sent job {} to retry topic (attempt {})", record.key(), newRetryCount);

        } catch (Exception e) {
            log.error("Failed to send to retry topic: {}", e.getMessage());
            // Si falla el envío a retry, enviar directamente a DLQ
            sendToDlq(record, e);
        }
    }

    /**
     * Calcular delay para próximo reintento (backoff exponencial)
     */
    private long calculateNextRetryDelay(int retryCount) {
        // Backoff exponencial: 1s, 5s, 15s, 30s, 60s
        switch (retryCount) {
            case 1: return 1000;    // 1 segundo
            case 2: return 5000;    // 5 segundos
            case 3: return 15000;   // 15 segundos
            case 4: return 30000;   // 30 segundos
            default: return 60000;  // 60 segundos
        }
    }

    /**
     * Enviar mensaje a Dead Letter Queue
     */
    private void sendToDlq(ConsumerRecord<String, JobRequest> record, Exception exception) {
        try {
            // Crear mensaje DLQ con información del error
            Map<String, Object> dlqPayload = new HashMap<>();
            dlqPayload.put("originalMessage", record.value());
            dlqPayload.put("error", exception.getMessage());
            dlqPayload.put("errorType", exception.getClass().getName());
            dlqPayload.put("stackTrace", getStackTraceAsString(exception));
            dlqPayload.put("originalTopic", record.topic());
            dlqPayload.put("originalPartition", record.partition());
            dlqPayload.put("originalOffset", record.offset());
            dlqPayload.put("originalTimestamp", record.timestamp());
            dlqPayload.put("dlqTimestamp", System.currentTimeMillis());
            dlqPayload.put("retryCount", getRetryCount(record));

            org.springframework.messaging.Message<Map<String, Object>> message =
                    org.springframework.messaging.support.MessageBuilder
                            .withPayload(dlqPayload)
                            .setHeader(org.springframework.kafka.support.KafkaHeaders.TOPIC,
                                    record.topic() + "-dlq")
                            .setHeader(org.springframework.kafka.support.KafkaHeaders.KEY, record.key())
                            .setHeader("dlq-reason", "max-retries-exceeded")
                            .setHeader("failure-timestamp", LocalDateTime.now().toString())
                            .build();

            kafkaTemplate.send(message);

            log.error("Sent job {} to DLQ. Error: {}", record.key(), exception.getMessage());

        } catch (Exception e) {
            log.error("Failed to send to DLQ: {}", e.getMessage());
            // En último recurso, loggear y perder el mensaje
            log.error("CRITICAL: Lost job {} due to DLQ failure. Original error: {}",
                    record.key(), exception.getMessage());
        }
    }

    private Integer getRetryCount(ConsumerRecord<String, JobRequest> record) {
        try {
            return Integer.parseInt(
                    new String(record.headers().lastHeader("retry-count").value())
            );
        } catch (Exception e) {
            return 0;
        }
    }

    private int getMaxRetries() {
        return 3; // Configurable
    }


    /**
     * Convertir stack trace a string
     */
    private String getStackTraceAsString(Exception exception) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        exception.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Listener para reprocesar mensajes de DLQ
     */
    @KafkaListener(
            topics = "${kafka.topics.job-requests}-dlq",
            containerFactory = "dlqListenerContainerFactory",
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

package org.example.batch.producer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import common.batch.dto.JobResult;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.bridge.Message;
import org.example.batch.model.PendingResult;
import org.example.batch.repository.PendingResultRepository;
import org.example.batch.service.JobMetricsService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobResultPublisher {

    @Value("${kafka.topics.job-results}")
    private String jobResultsTopic;

    @Value("${spring.application.name:unknown-service}")
    private String serviceName;

    private final KafkaTemplate<String, JobResult> kafkaTemplate;
    private final JobMetricsService metricsService;
    private final PendingResultRepository pendingResultRepository; // Asumiendo que existe

    /**
     * Publicar resultado de job de forma síncrona
     */
    @Transactional
    public void publishResult(JobResult result, String correlationId) {
        try {
            // Construir mensaje usando MessageBuilder con el tipo correcto
            var message = MessageBuilder
                    .withPayload(result)
                    .setHeader(KafkaHeaders.TOPIC, jobResultsTopic)
                    .setHeader(KafkaHeaders.KEY, result.getJobId())
                    .setHeader("correlation-id", correlationId != null ? correlationId : "unknown")
                    .setHeader("source-service", serviceName)
                    .setHeader("processed-at", LocalDateTime.now().toString())
                    .setHeader("result-status", result.getStatus())
                    .setHeader("publish-timestamp", System.currentTimeMillis())
                    .build();

            log.debug("Publishing result for job {} to topic {}",
                    result.getJobId(), jobResultsTopic);

            // Usar el método send correcto con Callback
            CompletableFuture<SendResult<String, JobResult>> future =
                    kafkaTemplate.send(message);

            // Manejar resultado asíncrono
            future.whenComplete((sendResult, ex) -> {
                if (ex != null) {
                    handlePublishFailure(result, ex);
                } else {
                    handlePublishSuccess(result, sendResult);
                }
            });

        } catch (Exception e) {
            log.error("Error publishing job result for job {}: {}",
                    result.getJobId(), e.getMessage(), e);

            // Guardar en tabla de resultados pendientes
            savePendingResult(result, correlationId, e.getMessage());

            // Registrar métrica de error
            metricsService.recordResultPublishFailed(result.getJobId());
        }
    }

    /**
     * Publicar resultado de forma síncrona (con timeout)
     */
    public boolean publishResultSync(JobResult result, String correlationId, long timeoutMs) {
        try {
            var message = MessageBuilder
                    .withPayload(result)
                    .setHeader(KafkaHeaders.TOPIC, jobResultsTopic)
                    .setHeader(KafkaHeaders.KEY, result.getJobId())
                    .setHeader("correlation-id", correlationId)
                    .setHeader("source-service", serviceName)
                    .build();

            // Envío síncrono con timeout
            SendResult<String, JobResult> sendResult =
                    kafkaTemplate.send(message).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);

            log.info("Result published synchronously for job {} to partition {}, offset {}",
                    result.getJobId(),
                    sendResult.getRecordMetadata().partition(),
                    sendResult.getRecordMetadata().offset());

            metricsService.recordResultPublished(result.getJobId());
            return true;

        } catch (Exception e) {
            log.error("Synchronous publish failed for job {}: {}",
                    result.getJobId(), e.getMessage());

            savePendingResult(result, correlationId, e.getMessage());
            metricsService.recordResultPublishFailed(result.getJobId());
            return false;
        }
    }

    /**
     * Publicar con reintentos
     */
    public void publishWithRetry(JobResult result, String correlationId, int maxRetries) {
        int attempt = 0;
        boolean published = false;

        while (attempt < maxRetries && !published) {
            attempt++;
            try {
                log.debug("Attempt {} to publish result for job {}", attempt, result.getJobId());

                published = publishResultSync(result, correlationId, 5000); // 5 segundos timeout

                if (!published) {
                    Thread.sleep(1000 * attempt); // Backoff exponencial
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.error("Publish retry interrupted for job {}", result.getJobId());
                break;
            } catch (Exception e) {
                log.warn("Retry {} failed for job {}: {}", attempt, result.getJobId(), e.getMessage());
            }
        }

        if (!published) {
            log.error("All {} retry attempts failed for job {}", maxRetries, result.getJobId());
            savePendingResult(result, correlationId, "Max retries exceeded");
        }
    }

    /**
     * Manejar éxito en publicación
     */
    private void handlePublishSuccess(JobResult result, SendResult<String, JobResult> sendResult) {
        log.info("✅ Result published successfully for job {} to topic {}, partition {}, offset {}",
                result.getJobId(),
                sendResult.getRecordMetadata().topic(),
                sendResult.getRecordMetadata().partition(),
                sendResult.getRecordMetadata().offset());

        metricsService.recordResultPublished(result.getJobId());

        // Si había un resultado pendiente, marcarlo como enviado
        markPendingResultAsSent(result.getJobId());
    }

    /**
     * Manejar fallo en publicación
     */
    private void handlePublishFailure(JobResult result, Throwable ex) {
        log.error("❌ Failed to publish result for job {}: {}",
                result.getJobId(), ex.getMessage(), ex);

        metricsService.recordResultPublishFailed(result.getJobId());

        // Intentar guardar como pendiente si es error transitorio
        if (isTransientError(ex)) {
            savePendingResult(result, null, ex.getMessage());
        }
    }

    /**
     * Guardar resultado pendiente en base de datos
     */
    @Transactional
    public void savePendingResult(JobResult result, String correlationId, String error) {
        try {
            PendingResult pendingResult = PendingResult.builder()
                    .jobId(result.getJobId())
                    .resultData(serializeResult(result))
                    .correlationId(correlationId)
                    .errorMessage(error)
                    .attempts(0)
                    .status("PENDING")
                    .createdAt(LocalDateTime.now())
                    .build();

            pendingResultRepository.save(pendingResult);

            log.info("Saved pending result for job {} to database", result.getJobId());

        } catch (Exception e) {
            log.error("Failed to save pending result for job {}: {}",
                    result.getJobId(), e.getMessage());
            // En último caso, loggear pero no fallar
        }
    }

    /**
     * Marcar resultado pendiente como enviado
     */
    @Transactional
    public void markPendingResultAsSent(String jobId) {
        try {
            pendingResultRepository.findByJobId(jobId).ifPresent(pendingResult -> {
                pendingResult.setStatus("SENT");
                pendingResult.setSentAt(LocalDateTime.now());
                pendingResultRepository.save(pendingResult);

                log.debug("Marked pending result for job {} as SENT", jobId);
            });
        } catch (Exception e) {
            log.warn("Could not mark pending result as sent for job {}: {}",
                    jobId, e.getMessage());
        }
    }

    /**
     * Reintentar enviar resultados pendientes
     */
    @Scheduled(fixedDelay = 300000) // Cada 5 minutos
    @Transactional
    public void retryPendingResults() {
        try {
            List<PendingResult> pendingResults = pendingResultRepository
                    .findByStatusAndAttemptsLessThan("PENDING", 3);

            log.info("Retrying {} pending results", pendingResults.size());

            for (PendingResult pending : pendingResults) {
                try {
                    JobResult result = deserializeResult(pending.getResultData());
                    result.setJobId(pending.getJobId()); // Asegurar ID

                    publishResult(result, pending.getCorrelationId());

                    pending.setAttempts(pending.getAttempts() + 1);
                    pendingResultRepository.save(pending);

                } catch (Exception e) {
                    log.error("Failed to retry pending result for job {}: {}",
                            pending.getJobId(), e.getMessage());

                    pending.setAttempts(pending.getAttempts() + 1);
                    if (pending.getAttempts() >= 3) {
                        pending.setStatus("FAILED");
                        pending.setErrorMessage("Max retries exceeded: " + e.getMessage());
                    }
                    pendingResultRepository.save(pending);
                }
            }

        } catch (Exception e) {
            log.error("Error in retryPendingResults: {}", e.getMessage(), e);
        }
    }

    /**
     * Serializar JobResult a JSON
     */
    private String serializeResult(JobResult result) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            return mapper.writeValueAsString(result);
        } catch (Exception e) {
            log.error("Failed to serialize JobResult: {}", e.getMessage());
            return "{}";
        }
    }

    /**
     * Deserializar JSON a JobResult
     */
    private JobResult deserializeResult(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            return mapper.readValue(json, JobResult.class);
        } catch (Exception e) {
            log.error("Failed to deserialize JobResult: {}", e.getMessage());
            return JobResult.builder()
                    .status("ERROR")
                    .message("Deserialization failed")
                    .build();
        }
    }

    /**
     * Determinar si el error es transitorio
     */
    private boolean isTransientError(Throwable ex) {
        String message = ex.getMessage().toLowerCase();
        return message.contains("timeout") ||
                message.contains("connection") ||
                message.contains("network") ||
                message.contains("unavailable") ||
                message.contains("retry");
    }

    /**
     * Obtener nombre del servicio
     */
    public String getServiceName() {
        return serviceName;
    }
}

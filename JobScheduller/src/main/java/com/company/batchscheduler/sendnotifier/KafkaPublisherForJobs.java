package com.company.batchscheduler.sendnotifier;

import common.batch.dto.JobRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.JobId;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaPublisherForJobs {

    @Value("${kafka.topics.job-requests}")
    private String jobRequestsTopic;

    private final JobScheduler jobScheduler;
    private final KafkaTemplate<String, JobRequest> kafkaTemplate;

    /**
     * Publica un evento de job y lo marca como SCHEDULED en JobRunr
     * @return jobrunrJobId para tracking
     */
    public String publishEventForRunJob(JobRequest request) {
        String jobId = request.getJobId();

        try {
            // 1. Crear job en JobRunr como SCHEDULED/ENQUEUED
            String jobrunrJobId = createScheduledJobInJobRunr(request);

            log.info("📅 Job {} scheduled in JobRunr - ID: {}", jobId, jobrunrJobId);

            // 2. Publicar a Kafka
            publishToKafka(request, jobrunrJobId);

            return jobrunrJobId;

        } catch (Exception e) {
            log.error("❌ Failed to schedule job {}: {}", jobId, e.getMessage(), e);
            throw new RuntimeException("Job scheduling failed", e);
        }
    }

    /**
     * Crear job SCHEDULED en JobRunr
     * Solo marca como SCHEDULED/ENQUEUED, NO como PROCESSING
     */
    private String createScheduledJobInJobRunr(JobRequest request) {
        String correlationId = generateCorrelationId();

        JobId jobRunrJobId;

        if (request.getScheduledAt() != null &&
                request.getScheduledAt().isAfter(LocalDateTime.now())) {
            // Job programado para futuro
            jobRunrJobId = jobScheduler.schedule(
                    request.getScheduledAt(),
                    () -> handleScheduledJobExecution(request.getJobId(), correlationId)
            );
            log.debug("Job {} scheduled for future execution at {}",
                    request.getJobId(), request.getScheduledAt());
        } else {
            // Job para ejecución inmediata
            jobRunrJobId = jobScheduler.enqueue(
                    () -> handleScheduledJobExecution(request.getJobId(), correlationId)
            );
            log.debug("Job {} enqueued for immediate execution", request.getJobId());
        }

        return jobRunrJobId.toString();
    }

    /**
     * Método que JobRunr ejecutará cuando sea tiempo
     * SOLO marca el job como "ejecutado por JobRunr", NO como processing/succeeded
     */
    public void handleScheduledJobExecution(String executorJobId, String correlationId) {
        // ⚠️ IMPORTANTE: Este método NO representa la ejecución real del job
        // Solo indica que JobRunr "disparó" el job hacia Kafka

        log.info("⏰ JobRunr triggered job {} for Kafka publishing", executorJobId);

        // Este método debería:
        // 1. Terminar rápidamente (el trabajo real lo hace el executor)
        // 2. NO lanzar excepciones a menos que falle el envío a Kafka
        // 3. JobRunr lo marcará como SUCCEEDED cuando termine sin error

        // NO intentamos marcar como PROCESSING aquí
        // El executor será quien notifique PROCESSING cuando realmente empiece
    }

    /**
     * Publicar a Kafka con todos los headers necesarios
     */
    private void publishToKafka(JobRequest request, String jobrunrJobId) {
        String correlationId = generateCorrelationId();

        Message<JobRequest> message = MessageBuilder
                .withPayload(request)
                // Headers obligatorios
                .setHeader(KafkaHeaders.TOPIC, jobRequestsTopic)
                .setHeader(KafkaHeaders.KEY, request.getJobId())
                .setHeader("jobrunr-job-id", jobrunrJobId)  // ⭐ Para que el executor lo use
                .setHeader("correlation-id", correlationId)

                // Headers para filtrado (DEBEN coincidir con el consumer)
                .setHeader("business-domain", request.getBusinessDomain())
                .setHeader("target-batch", request.getJobName())
                .setHeader("job-type", request.getJobType())

                // Headers de control
                .setHeader("priority", request.getPriority())
                .setHeader("retry-count", 0)
                .setHeader("scheduled-at", request.getScheduledAt() != null ?
                        request.getScheduledAt() : LocalDateTime.now())

                // Headers informativos
                .setHeader("source", "batch-scheduler-service")
                .setHeader("version", "1.0")
                .setHeader("producer-timestamp", System.currentTimeMillis())
                .setHeader("event-created-at", LocalDateTime.now())

                .build();

        // Enviar de forma asíncrona
        CompletableFuture<SendResult<String, JobRequest>> future =
                kafkaTemplate.send(message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                handleKafkaPublishFailure(request.getJobId(), jobrunrJobId, ex);
            } else {
                handleKafkaPublishSuccess(request.getJobId(), jobrunrJobId, result);
            }
        });
    }

    /**
     * Manejar éxito en publicación a Kafka
     */
    private void handleKafkaPublishSuccess(String jobId, String jobrunrJobId,
                                           SendResult<String, JobRequest> result) {
        log.info("""
                ✅ Job published to Kafka successfully:
                - Job ID: {}
                - JobRunr ID: {}
                - Topic: {}
                - Partition: {}
                - Offset: {}
                - Status in JobRunr: SCHEDULED/ENQUEUED
                """,
                jobId,
                jobrunrJobId,
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset()
        );

        // IMPORTANTE: JobRunr mantendrá el job como SCHEDULED/ENQUEUED
        // El executor será quien notifique los cambios de estado después
    }

    /**
     * Manejar fallo en publicación a Kafka
     * Si Kafka falla, necesitamos marcar el job como FAILED en JobRunr
     */
    private void handleKafkaPublishFailure(String jobId, String jobrunrJobId, Throwable ex) {
        log.error("""
                ❌ Kafka publish failed:
                - Job ID: {}
                - JobRunr ID: {}
                - Error: {}
                - Action: Marking as FAILED in JobRunr
                """,
                jobId, jobrunrJobId, ex.getMessage());

        // ⚠️ CRÍTICO: Si Kafka falla, el job nunca llegará al executor
        // Debemos marcarlo como FAILED en JobRunr
        markJobAsFailedInJobRunr(jobrunrJobId,
                "Failed to publish to Kafka: " + ex.getMessage());
    }

    /**
     * Marcar job como FAILED en JobRunr (solo para errores del scheduler)
     */
    private void markJobAsFailedInJobRunr(String jobrunrJobId, String error) {
        try {
            UUID uuid = UUID.fromString(jobrunrJobId);

            // Re-encolar el job para que falle explícitamente
            jobScheduler.enqueue(uuid, () -> {
                throw new RuntimeException("Scheduler error: " + error);
            });

            log.warn("JobRunr job {} marked as FAILED due to scheduler error",
                    jobrunrJobId);

        } catch (Exception e) {
            log.error("Could not mark JobRunr job as failed: {}", e.getMessage());
        }
    }


    /**
     * Generar correlation ID
     */
    private String generateCorrelationId() {
        return "corr-" + System.currentTimeMillis() + "-" +
                UUID.randomUUID().toString().substring(0, 8);
    }
}

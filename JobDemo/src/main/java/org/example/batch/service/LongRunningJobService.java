package org.example.batch.service;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.batch.job.CustomerSummaryJob;
import org.example.batch.model.PendingResult;
import org.example.batch.producer.JobResultPublisher;
import org.example.batch.repository.PendingResultRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Slf4j
@Service
@RequiredArgsConstructor
public class LongRunningJobService {

    private final CustomerSummaryJob jobExecutionService;
    private final JobResultPublisher jobResultPublisher;
    private final PendingResultRepository pendingResultRepository;

    @Qualifier("longRunningJobExecutor")
    private final ExecutorService longRunningJobExecutor;

    /**
     * Procesar job de larga duración (más de 30 minutos)
     */
    @Transactional
    public JobResult processLongRunningJob(JobRequest jobRequest,
                                           Map<String, String> headers,
                                           ConsumerRecord<String, JobRequest> record) {

        String jobId = jobRequest.getJobId();
        String correlationId = headers.get("correlation-id");

        log.info("🚀 Starting long-running job: {}, CorrelationId: {}, BusinessDomain: {}",
                jobId, correlationId, headers.get("business-domain"));

        try {
            // 1. Guardar como PENDING en base de datos
            PendingResult pendingResult = createPendingResult(jobRequest, headers, record);
            pendingResult.setStatus("IN_PROGRESS");
            pendingResult.setStartedAt(LocalDateTime.now());
            pendingResultRepository.save(pendingResult);

            // 2. Ejecutar asíncronamente sin timeout
            CompletableFuture.runAsync(() -> {
                try {
                    executeLongJobAsync(jobRequest, headers, pendingResult);
                } catch (Exception e) {
                    log.error("Critical error in async execution for job {}: {}",
                            jobId, e.getMessage(), e);
                    handleAsyncExecutionFailure(pendingResult, e);
                }
            }, longRunningJobExecutor);

            // 3. Retornar respuesta inmediata
            return createAcceptedResponse(jobRequest, correlationId, pendingResult.getId());

        } catch (Exception e) {
            log.error("Failed to start long-running job {}: {}", jobId, e.getMessage(), e);
            return createFailedResponse(jobRequest, correlationId, e);
        }
    }

    /**
     * Ejecutar job largo en segundo plano
     */
    private void executeLongJobAsync(JobRequest jobRequest,
                                     Map<String, String> headers,
                                     PendingResult pendingResult) {

        String jobId = jobRequest.getJobId();

        try {
            log.info("⚙️ Executing long-running job in background: {}", jobId);

            // Actualizar heartbeat periódicamente
            startHeartbeatThread(pendingResult);

            // Ejecutar el job (puede tardar horas)
            JobResult jobResult = jobExecutionService.executeJob(jobRequest, headers);

            // Detener heartbeat
            stopHeartbeat(pendingResult);

            // Actualizar resultado
            updatePendingResultWithSuccess(pendingResult, jobResult);

            // Publicar resultado
            publishJobResult(jobResult, pendingResult.getCorrelationId());

            log.info("✅ Long-running job completed successfully: {}", jobId);

        } catch (Exception e) {
            log.error("❌ Long-running job failed: {}, Error: {}", jobId, e.getMessage(), e);
            updatePendingResultWithFailure(pendingResult, e);

            // Publicar resultado fallido
            publishFailedResult(jobRequest, pendingResult.getCorrelationId(), e);
        }
    }

    /**
     * Crear registro PendingResult
     */
    private PendingResult createPendingResult(JobRequest jobRequest,
                                              Map<String, String> headers,
                                              ConsumerRecord<String, JobRequest> record) {

        return PendingResult.builder()
                .jobId(jobRequest.getJobId())
                .correlationId(headers.get("correlation-id"))
                .businessDomain(headers.get("business-domain"))
                .targetBatch(headers.get("target-batch"))
                .priority(headers.get("priority"))
                .jobType("LONG_RUNNING")
                .status("PENDING")
                .attempts(0)
                .estimatedDurationMinutes(estimateDuration(jobRequest))
                .startedAt(LocalDateTime.now())
                .expectedCompletionAt(LocalDateTime.now().plusMinutes(estimateDuration(jobRequest)))
                .kafkaTopic(record.topic())
                .kafkaPartition(record.partition())
                .kafkaOffset(record.offset())
                .build();
    }

    /**
     * Estimación de duración del job (en minutos)
     */
    private int estimateDuration(JobRequest jobRequest) {
        // Lógica basada en el tipo de job
        switch (jobRequest.getJobName()) {
            case "DAILY_REPORT":
                return 120; // 2 horas
            case "MONTHLY_ANALYSIS":
                return 480; // 8 horas
            case "YEARLY_SUMMARY":
                return 1440; // 24 horas
            default:
                return 60; // 1 hora por defecto
        }
    }

    /**
     * Respuesta de aceptación inmediata
     */
    private JobResult createAcceptedResponse(JobRequest jobRequest,
                                             String correlationId,
                                             Long pendingResultId) {

        return JobResult.builder()
                .jobId(jobRequest.getJobId())
                .jobName(jobRequest.getJobName())
                .status(JobStatusEnum.ACCEPTED)
                .message("Job accepted for long-running processing. " +
                        "Check status with tracking ID: " + pendingResultId)
                .startedAt(LocalDateTime.now())
                .correlationId(correlationId)
                .trackingId(String.valueOf(pendingResultId))
                .asyncJob(true)
                .estimatedCompletionMinutes(estimateDuration(jobRequest))
                .build();
    }

    /**
     * Respuesta de fallo inmediato
     */
    private JobResult createFailedResponse(JobRequest jobRequest,
                                           String correlationId,
                                           Exception e) {

        return JobResult.builder()
                .jobId(jobRequest.getJobId())
                .jobName(jobRequest.getJobName())
                .status(JobStatusEnum.FAILED)
                .message("Failed to start long-running job")
                .startedAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .errorDetails(e.getMessage())
                .correlationId(correlationId)
                .build();
    }

    /**
     * Actualizar resultado exitoso
     */
    @Transactional
    private void updatePendingResultWithSuccess(PendingResult pendingResult,
                                                JobResult jobResult) {

        pendingResult.setStatus("COMPLETED");
        pendingResult.setCompletedAt(LocalDateTime.now());
        pendingResult.setResultData(serializeJobResult(jobResult));
        pendingResultRepository.save(pendingResult);
    }

    /**
     * Actualizar resultado fallido
     */
    @Transactional
    private void updatePendingResultWithFailure(PendingResult pendingResult,
                                                Exception e) {

        pendingResult.setStatus("FAILED");
        pendingResult.setCompletedAt(LocalDateTime.now());
        pendingResult.setErrorMessage(e.getMessage());
        pendingResult.incrementAttempts();
        pendingResultRepository.save(pendingResult);
    }

    /**
     * Publicar resultado exitoso
     */
    private void publishJobResult(JobResult jobResult, String correlationId) {
        try {
            // Usar el publisher existente
            jobResultPublisher.publishResult(jobResult, correlationId);

            // Marcar como enviado
            updatePendingResultAsSent(jobResult.getJobId());

        } catch (Exception e) {
            log.error("Failed to publish result for job {}: {}",
                    jobResult.getJobId(), e.getMessage());
            // El publisher ya maneja resultados pendientes
        }
    }

    /**
     * Publicar resultado fallido
     */
    private void publishFailedResult(JobRequest jobRequest,
                                     String correlationId,
                                     Exception e) {

        JobResult failedResult = JobResult.builder()
                .jobId(jobRequest.getJobId())
                .jobName(jobRequest.getJobName())
                .status(JobStatusEnum.FAILED)
                .message("Long-running job execution failed")
                .startedAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .errorDetails(e.getMessage())
                .correlationId(correlationId)
                .build();

        jobResultPublisher.publishResult(failedResult, correlationId);
    }

    /**
     * Heartbeat para jobs largos
     */
    private void startHeartbeatThread(PendingResult pendingResult) {
        Thread heartbeatThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(300000); // 5 minutos

                    // Actualizar timestamp de actualización
                    pendingResultRepository.findById(pendingResult.getId())
                            .ifPresent(pr -> {
                                pr.setUpdatedAt(LocalDateTime.now());
                                pendingResultRepository.save(pr);
                                log.debug("Heartbeat for job {}", pr.getJobId());
                            });

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        heartbeatThread.setName("heartbeat-" + pendingResult.getJobId());
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();

        pendingResult.setUpdatedAt(LocalDateTime.now());
        pendingResultRepository.save(pendingResult);
    }

    private void stopHeartbeat(PendingResult pendingResult) {
        // Implementar lógica para detener heartbeat si es necesario
        log.debug("Stopping heartbeat for job {}", pendingResult.getJobId());
    }

    /**
     * Manejar fallo en ejecución async
     */
    private void handleAsyncExecutionFailure(PendingResult pendingResult, Exception e) {
        log.error("Async execution failed for job {}: {}",
                pendingResult.getJobId(), e.getMessage(), e);

        updatePendingResultWithFailure(pendingResult, e);

        // Intentar reintento si aplica
        if (pendingResult.canRetry()) {
            scheduleRetry(pendingResult);
        }
    }

    /**
     * Programar reintento
     */
    private void scheduleRetry(PendingResult pendingResult) {
        log.info("Scheduling retry for job {} in 5 minutes", pendingResult.getJobId());

        // Podrías usar @Scheduled o un job scheduler
        CompletableFuture.delayedExecutor(5, java.util.concurrent.TimeUnit.MINUTES)
                .execute(() -> {
                    retryLongRunningJob(pendingResult);
                });
    }

    /**
     * Reintentar job largo
     */
    @Transactional
    public void retryLongRunningJob(PendingResult pendingResult) {
        try {
            log.info("Retrying long-running job: {}", pendingResult.getJobId());

            // Deserializar JobRequest (necesitarías guardarlo también)
            // JobRequest jobRequest = deserializeJobRequest(pendingResult.getRequestData());

            // Por ahora, marcamos como reintentado
            pendingResult.incrementAttempts();
            pendingResult.setStatus("PENDING");
            pendingResult.setUpdatedAt(LocalDateTime.now());
            pendingResultRepository.save(pendingResult);

            // Nota: Necesitarías guardar el JobRequest original también

        } catch (Exception e) {
            log.error("Failed to retry job {}: {}",
                    pendingResult.getJobId(), e.getMessage());
        }
    }

    /**
     * Serializar JobResult
     */
    private String serializeJobResult(JobResult result) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
            mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            return mapper.writeValueAsString(result);
        } catch (Exception e) {
            log.error("Failed to serialize JobResult: {}", e.getMessage());
            return "{\"status\":\"SERIALIZATION_ERROR\"}";
        }
    }

    /**
     * Marcar como enviado
     */
    @Transactional
    private void updatePendingResultAsSent(String jobId) {
        pendingResultRepository.findByJobId(jobId).ifPresent(pendingResult -> {
            pendingResult.setStatus("SENT");
            pendingResult.setSentAt(LocalDateTime.now());
            pendingResultRepository.save(pendingResult);
            log.debug("Marked pending result for job {} as SENT", jobId);
        });
    }
}

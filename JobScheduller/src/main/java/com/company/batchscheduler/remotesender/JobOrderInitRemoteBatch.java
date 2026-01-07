package com.company.batchscheduler.remotesender;

import com.company.batchscheduler.service.JobManagementOperations;
import common.batch.dto.JobRequest;
import common.batch.dto.JobType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
@Slf4j
@Component
public class JobOrderInitRemoteBatch {

    @Value("${kafka.topics.job-requests}")
    private String jobRequestsTopic;

    private final KafkaTemplate<String, JobRequest> kafkaTemplate;
    private final JobManagementOperations jobManagementOperations;

    @Job(name= "Async Job")
    public void dispararJobRemoto(JobRequest request, JobContext jobContext) {

        jobContext.saveMetadata("remote", "true");
        jobContext.saveMetadata("nombre-Job", request.getJobName());

        request.setScheduledAt(LocalDateTime.now());
        if (request.getJobId() == null) {
            request.setJobId(jobContext.getJobId().toString());
        } else if (!request.getJobId().contentEquals(jobContext.getJobId().toString())) {
            throw new RuntimeException("Atención: Discrepancia entre el requestId y el jobContextId!");
        }
        this.sendToRemoteWorker(request);

        // Guardar metadata para tracking
        jobContext.saveMetadata("progress", 25);
        jobContext.saveMetadata("progressMessage", "Processing data...");
        jobContext.saveMetadata("lastUpdate", Instant.now());
        jobContext.saveMetadata("remoteWorkerNotified", true);
        jobContext.saveMetadata("expectedCompletion",
                LocalDateTime.now().plusHours(2).toString());

        log.info("Job {} is IN_PROGRESS", request.getJobId());

    }


    /**
     * Construye mensaje con headers de routing para filtrado
     */
    private void sendToRemoteWorker(JobRequest request) {

        // ID del JOB del ID del padre
        String correlationId = this.jobManagementOperations.getRecurringIdJobByName(request.getJobName());

        log.info("🎯 JobRunr Job created - For Executor Job with ID: {}", request.getJobId());

        Message<JobRequest> message = MessageBuilder
                .withPayload(request)
                // Headers principales para routing
                .setHeader(KafkaHeaders.TOPIC, jobRequestsTopic)
                .setHeader(KafkaHeaders.KEY, request.getJobId())
                .setHeader("job-id", request.getJobId())
                .setHeader("jobrunr-job-id", request.getJobId())
                // Headers de routing/filtrado
                .setHeader("job-type", request.getJobType())          // "ASYNCRONOUS"
                .setHeader("business-domain", request.getBusinessDomain()) // Ej: "application-job-demo"
                .setHeader((JobType.BATCH_PROCESSING.name().contentEquals(request.getJobType()) ? "target-batch"
                        : "target-job"),
                        request.getJobName()) // Ej: "ResumenDiarioClientesAsync"

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

        // Publicar a Kafka
        CompletableFuture<SendResult<String, JobRequest>> future = kafkaTemplate.send(message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                handlePublishFailure(request.getJobId(), ex);
            } else {
                handlePublishSuccess(request.getJobId(), result);
            }
        });
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
    }

    /**
     * Maneja fallo en publicación
     */
    private void handlePublishFailure(String jobId, Throwable ex) {
        log.error("Failed to publish job {} to Kafka: {}", jobId, ex.getMessage());
    }


}

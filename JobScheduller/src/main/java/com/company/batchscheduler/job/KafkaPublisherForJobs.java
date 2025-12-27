package com.company.batchscheduler.job;

import common.batch.dto.JobRequest;
import common.batch.dto.JobType;
import common.batch.model.JobStatus;
import common.batch.repository.JobStatusRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
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

    private final JobStatusRepository statusRepository;

    private final KafkaTemplate<String, JobRequest> kafkaTemplate;

    public JobStatus publishEventForRunJob(String jobId, JobRequest request) {
        // Guardar estado inicial en BD
        JobStatus status = JobStatus.builder()
                .jobId(jobId)
                .jobType(JobType.ASYNCRONOUS.toString())
                .status("ENQUEUED")
                .message("Job enqueued for execution")
                .createdAt(LocalDateTime.now())
                .build();
        statusRepository.save(status);

        // Publicar mensaje a Kafka usando CompletableFuture
        try {
            CompletableFuture<SendResult<String, JobRequest>> future =
                    kafkaTemplate.send(jobRequestsTopic, jobId, request);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish job {} to Kafka: {}",
                            jobId, ex.getMessage());
                    // Actualizar estado a FAILED de forma asíncrona
                    updateJobStatus(jobId, "FAILED",
                            "Failed to enqueue job: " + ex.getMessage());
                } else {
                    log.info("Job {} published to Kafka successfully. Offset: {}",
                            jobId, result.getRecordMetadata().offset());
                }
            });

        } catch (Exception e) {
            log.error("Error sending to Kafka: {}", e.getMessage());
            status.setStatus("FAILED");
            status.setMessage("Failed to enqueue job: " + e.getMessage());
            statusRepository.save(status);
        }
        return status;
    }

    // Método helper para actualizar estado de forma asíncrona
    @Async
    public void updateJobStatus(String jobId, String status, String message) {
        statusRepository.findByJobId(jobId).ifPresent(jobStatus -> {
            jobStatus.setStatus(status);
            jobStatus.setMessage(message);
            jobStatus.setUpdatedAt(LocalDateTime.now());
            statusRepository.save(jobStatus);
        });
    }


}

package org.example.batch.consumer;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.batch.job.CustomerSummaryJob;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobRequestConsumer {

    private final CustomerSummaryJob jobExecutorService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.job-results}")
    private String jobResultsTopic;

    @KafkaListener(topics = "${kafka.topics.job-requests}", groupId = "job-executor-group")
    public void consumeJobRequest(JobRequest request) {
        log.info("Processing job request: {}", request.getJobId());

        try {
            // Ejecutar el job (puede tardar horas - esto es asíncrono dentro del listener)
            String resultMessage = jobExecutorService.generateDailySummary(request);

            // Publicar resultado exitoso
            JobResult jobResult = new JobResult(
                    request.getJobId(),
                    true,
                    resultMessage,
                    LocalDateTime.now()
            );
            jobResult.setJobType(request.getJobType().toString());

            kafkaTemplate.send(jobResultsTopic, request.getJobId(), jobResult);
            log.info("Job {} completed successfully", request.getJobId());

        } catch (Exception e) {
            log.error("Error executing job {}: {}", request.getJobId(), e.getMessage(), e);

            // Publicar resultado fallido
            JobResult jobResult = new JobResult(
                    request.getJobId(),
                    false,
                    "Error: " + e.getMessage(),
                    LocalDateTime.now()
            );
            jobResult.setJobType(request.getJobType().toString());

            kafkaTemplate.send(jobResultsTopic, request.getJobId(), jobResult);
        }
    }
}

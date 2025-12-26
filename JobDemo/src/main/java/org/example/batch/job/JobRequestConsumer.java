package org.example.batch.job;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@Slf4j
public class JobRequestConsumer {

    @Autowired
    private CustomerSummaryJob customerSummaryJob;

    @Autowired
    private KafkaTemplate<String, JobResult> kafkaTemplate;

    @KafkaListener(topics = "job-requests", groupId = "job-executor-group")
    public void consumeJobRequest(JobRequest request) {
        log.info("Processing job request: {}", request.getJobId());

        try {
            // Ejecutar el job (puede tardar horas)
            String result = customerSummaryJob.generateDailySummary(request);

            // Publicar resultado exitoso
            JobResult jobResult = new JobResult(
                    request.getJobId(),
                    true,
                    result,
                    LocalDateTime.now()
            );
            kafkaTemplate.send("job-results", request.getJobId(), jobResult);

        } catch (Exception e) {
            // Publicar resultado fallido
            JobResult jobResult = new JobResult(
                    request.getJobId(),
                    false,
                    "Error: " + e.getMessage(),
                    LocalDateTime.now()
            );
            kafkaTemplate.send("job-results", request.getJobId(), jobResult);
        }
    }
}

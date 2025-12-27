package com.company.batchscheduler.consumer;

import com.company.batchscheduler.repository.JobStatusRepository;
import common.batch.dto.JobResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
@Slf4j
public class JobResultConsumer {

    private final JobStatusRepository statusRepository;

    @KafkaListener(topics = "${kafka.topics.job-results}", groupId = "scheduler-group")
    public void consumeJobResult(JobResult result) {
        log.info("Received job result for jobId: {}", result.getJobId());

        statusRepository.findByJobId(result.getJobId()).ifPresentOrElse(
                status -> {
                    status.setStatus(result.getSuccess() ? "COMPLETED" : "FAILED");
                    status.setMessage(result.getMessage());
                    status.setFinishedAt(result.getCompletedAt());
                    statusRepository.save(status);
                    log.info("Updated status for job {} to {}", result.getJobId(), status.getStatus());
                },
                () -> log.warn("No job status found for jobId: {}", result.getJobId())
        );
    }

}

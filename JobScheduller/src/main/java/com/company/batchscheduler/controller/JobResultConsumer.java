package com.company.batchscheduler.controller;

import common.batch.dto.JobResult;
import common.batch.model.JobStatus;
import common.batch.repository.JobStatusRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@Slf4j
public class JobResultConsumer {

    @Autowired
    private JobStatusRepository statusRepository;

    @KafkaListener(topics = "job-results", groupId = "scheduler-group")
    public void consumeJobResult(JobResult result) {
        log.info("Received job result: {}", result.getJobId());

        JobStatus status = statusRepository.findByJobId(result.getJobId())
                .orElse(new JobStatus());

        status.setStatus(result.isSuccess() ? "COMPLETED" : "FAILED");
        status.setFinishedAt(LocalDateTime.now());
        status.setMessage(result.getMessage());
        statusRepository.save(status);
    }

}

package com.ad.muface.batch.scheduler.events;

import com.ad.muface.batch.dto.JobResult;
import com.ad.muface.batch.dto.JobStatusEnum;
import com.ad.muface.batch.scheduler.service.JobManagementOperations;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.Job;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobResultConsumer {

    private final JobManagementOperations jobManagementOperations;

    @KafkaListener(
            topics = "${kafka.topics.job-results}",
            groupId = "${spring.application.name}-results-consumer"
    )
    public void consumeJobResult(JobResult result) {

        try {
            String jobrunrJobIdStr = result.getJobId();
            int indexHasta = jobrunrJobIdStr.indexOf("instanceId");
            if (indexHasta != -1) {
                jobrunrJobIdStr = jobrunrJobIdStr.substring(0, indexHasta - 1);
            }
            UUID uuid = UUID.fromString(jobrunrJobIdStr == null ? "unknown" : jobrunrJobIdStr);
            Job job = jobManagementOperations.getJobById(uuid);
            if (job == null) {
                log.warn("JOB SCHEDULER::: JobRunr job {} not found in storage or was deleted", jobrunrJobIdStr);
            } else {
                log.info("📨 JOB SCHEDULER:::Received job {} for JobRunr Job ID: {}, result: {}, Status: {}",
                        job.getJobName(), result.getJobId(),
                        result.getStatus().toString().contentEquals(JobStatusEnum.FAILED.toString())
                                ? result.getErrorDetails()
                                : result.getMessage(),
                        result.getStatus());

                jobManagementOperations.updateJobRunrStatus(job, result);

            }

        } catch (Exception e) {
            log.error("Error processing job result for {}: {}", result.getJobId(), e.getMessage(), e);
        }
    }


}

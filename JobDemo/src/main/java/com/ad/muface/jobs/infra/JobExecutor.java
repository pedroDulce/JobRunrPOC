package com.ad.muface.jobs.infra;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public abstract class JobExecutor {
    protected final HeartbeatService heartbeatService;

    protected JobExecutor(HeartbeatService heartbeatService) {
        this.heartbeatService = heartbeatService;
    }

    public JobResult executeJob(JobRequest jobRequest) {
        try {
            heartbeatService.startHeartbeat(jobRequest);
            return executeJobLogic(jobRequest);
        } catch (Exception e) {
            throw e;
        } finally {
            heartbeatService.stopHeartbeat(jobRequest.getJobRunnerId());
        }
    }


    public abstract JobResult executeJobLogic(JobRequest jobRequest);

    protected JobResult buildJobResult(JobRequest jobRequest, Long duration, String message) {
        JobResult resultado = JobResult.builder()
                .jobId(jobRequest.getJobId())
                .jobName(jobRequest.getJobName())
                .status(JobStatusEnum.COMPLETED)
                .message(message)
                .startedAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .errorDetails(null)
                .durationMs(duration)
                .correlationId(jobRequest.getCorrelationId())
                .jobrunrJobId(jobRequest.getJobId())  // IMPORTANTE: ID de JobRunr
                .build();

        return resultado;

    }

}
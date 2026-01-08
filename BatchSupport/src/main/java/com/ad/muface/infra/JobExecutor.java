package com.ad.muface.infra;

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

    protected JobResult buildJobSuccessResult(JobRequest jobRequest, Long duration, String finishMessage) {
        return buildJobResult(jobRequest, duration, finishMessage, null);
    }

    protected JobResult buildJobFailedResult(JobRequest jobRequest, Long duration, String finishMessage, String errorDetails) {
        if (errorDetails == null) {
            throw new IllegalArgumentException("errorDetails debe ser una cadena de texto no nula");
        }
        return buildJobResult(jobRequest, duration, finishMessage, errorDetails);
    }

    private JobResult buildJobResult(JobRequest jobRequest, Long duration, String finishMessage, String errorDetails) {
        JobResult resultado = JobResult.builder()
                .jobId(jobRequest.getJobId())
                .jobName(jobRequest.getJobName())
                .status(errorDetails != null ? JobStatusEnum.FAILED : JobStatusEnum.COMPLETED)
                .message(finishMessage)
                .startedAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .errorDetails(errorDetails)
                .durationMs(duration)
                .correlationId(jobRequest.getCorrelationId())
                .jobrunrJobId(jobRequest.getJobId())  // IMPORTANTE: ID de JobRunr
                .build();

        return resultado;

    }

}
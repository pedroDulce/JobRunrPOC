package com.ad.muface.jobs.infra;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import lombok.Data;

@Data
public abstract class JobExecutor {
    protected final HeartbeatService heartbeatService;
    protected String jobId;

    protected JobExecutor(HeartbeatService heartbeatService) {
        this.heartbeatService = heartbeatService;
    }

    public JobResult executeJob(JobRequest jobRequest) {
        try {
            heartbeatService.startHeartbeat(jobId, jobRequest);
            return executeJob(jobRequest);
        } finally {
            heartbeatService.stopHeartbeat(jobId);
        }
    }

    public abstract JobResult executeJobLogic(JobRequest jobRequest);
}
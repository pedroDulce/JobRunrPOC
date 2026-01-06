package com.ad.muface.jobs.infra;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;

public abstract class JobExecutor {
    protected final HeartbeatService heartbeatService;
    protected final String jobId;

    protected JobExecutor(String jobId, HeartbeatService heartbeatService) {
        this.jobId = jobId;
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
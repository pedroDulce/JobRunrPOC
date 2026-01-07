package com.ad.muface.jobs.infra;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import lombok.Data;

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

}
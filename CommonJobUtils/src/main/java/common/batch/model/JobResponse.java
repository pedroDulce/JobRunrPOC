package common.batch.model;

import lombok.Data;

@Data
public class JobResponse {

    private String jobId;
    private String jobEnqueuedForExecution;

    public JobResponse(String jobId, String jobEnqueuedForExecution) {
        this.jobId = jobId;
        this.jobEnqueuedForExecution = jobEnqueuedForExecution;
    }
}

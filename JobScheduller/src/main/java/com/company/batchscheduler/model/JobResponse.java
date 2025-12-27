package com.company.batchscheduler.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class JobResponse {

    private String jobId;
    private String status;
    private String message;
    private LocalDateTime timestamp;

    public JobResponse(String jobId, String status, String message, LocalDateTime timestamp) {
        this.jobId = jobId;
        this.status = status;
        this.message = message;
        this.timestamp = timestamp;
    }

}

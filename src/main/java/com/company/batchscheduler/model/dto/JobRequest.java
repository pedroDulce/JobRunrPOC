package com.company.batchscheduler.model.dto;

import lombok.Data;

/** TODO: meter esta clase en la infrabackend **/
@Data
public class JobRequest {

    private String jobType;
    private String parametersJson;

    public JobRequest(String jobType, String parametersJson) {
        this.jobType = jobType;
        this.parametersJson = parametersJson;
    }

}

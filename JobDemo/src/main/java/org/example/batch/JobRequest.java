package org.example.batch;

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

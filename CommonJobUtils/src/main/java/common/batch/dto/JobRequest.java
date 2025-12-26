package common.batch.dto;

import lombok.Data;

/** TODO: meter esta clase en la infrabackend **/
@Data
public class JobRequest {

    private String jobType;
    private String jobName;
    private String parametersJson;
    private String cronExpression;

    public JobRequest(String jobName, String jobType, String parametersJson) {
        this.jobName = jobName;
        this.jobType = jobType;
        this.parametersJson = parametersJson;
    }

}

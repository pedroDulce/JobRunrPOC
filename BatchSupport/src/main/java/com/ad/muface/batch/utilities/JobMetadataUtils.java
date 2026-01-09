package com.ad.muface.batch.utilities;

import com.ad.muface.batch.dto.JobRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
public class JobMetadataUtils {

    public static JobParameters createJobRequestParameters(JobRequest jobRequest) {
        JobParametersBuilder paramsBuilder = new JobParametersBuilder();
        if (jobRequest.getParameters() != null && !jobRequest.getParameters().isEmpty()) {
            for (Map.Entry<String, String> entry : jobRequest.getParameters().entrySet()) {
                paramsBuilder.addString(entry.getKey(), entry.getValue());
                log.debug("param.key:: " + entry.getKey() + " - param.value:: " + entry.getValue());
            }
        }
        paramsBuilder.addString("externalJobId", jobRequest.getJobId())
                .addString("jobName", jobRequest.getJobName())
                .addString("jobCorrelationId", jobRequest.getCorrelationId() == null ? "no planned job" : jobRequest.getCorrelationId())
                .addString("executionTime", LocalDateTime.now().toString())
                .addLong("timestamp", System.currentTimeMillis(), true);
        return paramsBuilder.toJobParameters();
    }

}

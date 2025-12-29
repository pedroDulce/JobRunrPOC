package common.batch.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobResult {

    private String jobId;
    private String jobName;
    private JobStatusEnum status;
    private String message;
    private Object resultData;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private Long durationMs;
    private String errorDetails;
    private Map<String, Object> metadata;
    private String correlationId;
    private String jobrunrJobId;
    private Boolean asyncJob;
    private Integer estimatedCompletionMinutes;

    public JobResult(String jobId, JobStatusEnum status, String result, LocalDateTime completedAt) {
        this.completedAt = completedAt;
        this.status = status;
        this.message = result;
        this.jobId = jobId;
    }

    public boolean isSuccess() {
        return "SUCCESS".equals(status);
    }

    public boolean isFailed() {
        return "FAILED".equals(status);
    }


    // Método helper
    public boolean isFinalState() {
        return status == JobStatusEnum.SUCCESS ||
                status == JobStatusEnum.FAILED ||
                status == JobStatusEnum.CANCELLED;
    }

}




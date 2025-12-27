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
    private JobStatusEnum status; // SUCCESS, FAILED, CANCELLED
    private String message;
    private Object resultData;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private Long durationMs;
    private String errorDetails;
    private Map<String, Object> metadata;
    private String correlationId;

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


}




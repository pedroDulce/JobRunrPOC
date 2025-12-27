package common.batch.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobRequest implements Serializable {
    private String jobId;
    private String jobName;
    private String cronExpression;
    private String correlationId;
    private String source;
    private Map<String, String> parameters;
    private LocalDateTime scheduledAt;

    // Campos para routing/filtrado
    private String jobType;           // Ej: "CUSTOMER_SUMMARY", "BILLING_PROCESS"
    private String category;          // Ej: "REPORTING", "MAINTENANCE", "SYNC"
    private String targetService;     // Ej: "customer-service", "billing-service"
    private String businessDomain;    // Ej: "SALES", "FINANCE", "INVENTORY"
    private String priority;          // Ej: "HIGH", "MEDIUM", "LOW"
    private LocalDateTime ttl;        // Time To Live (expiración)

    // Metadata adicional
    private String createdBy;
    private List<String> requiredCapabilities; // Ej: ["REPORT_GENERATION", "EMAIL_SENDING"]
    private Map<String, String> labels;        // Labels para filtrado avanzado
    private Integer maxRetries;
    private Integer timeoutSeconds;

    private Map<String, Object> metadata;

    public JobRequest(String jobId, String jobType, Map<String, String> parameters) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.parameters = parameters;
    }
}
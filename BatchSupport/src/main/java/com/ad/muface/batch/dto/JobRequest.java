package com.ad.muface.batch.dto;

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
    private String businessDomain;
    private String cronExpression;
    private String correlationId;
    private Map<String, String> parameters;
    private LocalDateTime scheduledAt;
    private Integer heartBeatLapse = 5; // en segundos, valor por defecto para la PoC

    // Campos para routing/filtrado
    private String jobType;           // Ej: "SYNCRONOUS" o "ASYNCRONOUS"
    private String priority;          // Ej: "HIGH", "MEDIUM", "LOW"
    private LocalDateTime ttl;        // Time To Live (expiración)

    // Metadata adicional
    private String createdBy;
    private List<String> requiredCapabilities; // Ej: ["REPORT_GENERATION", "EMAIL_SENDING"]
    private Map<String, String> labels;        // Labels para filtrado avanzado
    private Integer maxRetries;
    private Integer timeoutSeconds;

    private Map<String, Object> metadata;

    private String jobRunnerId;

    public JobRequest(String jobId, String jobType, Map<String, String> parameters) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.parameters = parameters;
    }
}
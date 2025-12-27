package common.batch.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobRequest implements Serializable {

    // Identificador único del job (generado automáticamente si no se proporciona)
    private String jobId;

    private String jobName;

    private String cronExpression;

    // Tipo de job a ejecutar (LARGO (solo enfoque asíncrono, SHORT, puede invocarse de forma síncrona o asíncrona)
    private JobType jobType;

    // Fecha y hora de solicitud
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime processDate;

    // Prioridad del job (1-alta, 2-media, 3-baja)
    private Integer priority;

    // Usuario que solicita el job
    private String requestedBy;

    // URL de callback para notificaciones (opcional)
    private String callbackUrl;

    // Timeout en minutos (0 = sin timeout)
    private Integer timeoutMinutes;

    // Metadata adicional
    private Map<String, String> parameters;

    public JobRequest(String jobId, JobType jobType, Map<String, String> parameters) {
        this.jobId = jobId != null ? jobId : UUID.randomUUID().toString();
        this.jobType = jobType;
        this.parameters = parameters;
    }

    public JobRequest(String jobId, JobType jobType, String requestedBy, Integer priority) {
        this.jobId = jobId != null ? jobId : UUID.randomUUID().toString();
        this.jobType = jobType;
        this.requestedBy = requestedBy;
        this.priority = priority != null ? priority : 2;
        this.processDate = LocalDateTime.now();
    }

    // Método helper para crear JobRequest con prioridad alta
    public static JobRequest highPriority(JobType jobType, String parametersJson, String requestedBy) {
        return JobRequest.builder()
                .jobId(UUID.randomUUID().toString())
                .jobType(jobType)
                .requestedBy(requestedBy)
                .priority(1)
                .processDate(LocalDateTime.now())
                .build();
    }

    // Método helper para crear JobRequest con callback
    public static JobRequest withCallback(JobType jobType, String parametersJson,
                                          String callbackUrl) {
        return JobRequest.builder()
                .jobId(UUID.randomUUID().toString())
                .jobType(jobType)
                .callbackUrl(callbackUrl)
                .processDate(LocalDateTime.now())
                .priority(2)
                .build();
    }

    // Método para obtener parámetro específico del mapa
    public String getParameter(String key) {
        return parameters != null ? parameters.get(key) : null;
    }

    // Método para obtener parámetro específico con valor por defecto
    public String getParameter(String key, String defaultValue) {
        return parameters != null ? parameters.getOrDefault(key, defaultValue) : defaultValue;
    }

    // Validación básica del request
    public boolean isValid() {
        return jobId != null && !jobId.trim().isEmpty();
    }
}

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

    private String cronExpression;

    // Tipo de job a ejecutar (LARGO (solo enfoque asíncrono, SHORT, puede invocarse de forma síncrona o asíncrona)
    private JobType jobType;

    // Parámetros del job en formato JSON
    private String parametersJson;

    // Parámetros del job como mapa (alternativa a JSON)
    private Map<String, Object> parameters;

    // Fecha y hora de solicitud
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime requestedAt;

    // Prioridad del job (1-alta, 2-media, 3-baja)
    private Integer priority;

    // Usuario que solicita el job
    private String requestedBy;

    // URL de callback para notificaciones (opcional)
    private String callbackUrl;

    // Timeout en minutos (0 = sin timeout)
    private Integer timeoutMinutes;

    // Metadata adicional
    private Map<String, String> metadata;

    public JobRequest(String jobId, JobType jobType, String parametersJson) {
        this.jobId = jobId != null ? jobId : UUID.randomUUID().toString();
        this.jobType = jobType;
        this.parametersJson = parametersJson;
    }

    public JobRequest(String jobId, JobType jobType, String parametersJson,
                      String requestedBy, Integer priority) {
        this.jobId = jobId != null ? jobId : UUID.randomUUID().toString();
        this.jobType = jobType;
        this.parametersJson = parametersJson;
        this.requestedBy = requestedBy;
        this.priority = priority != null ? priority : 2;
        this.requestedAt = LocalDateTime.now();
    }

    // Método helper para crear JobRequest con prioridad alta
    public static JobRequest highPriority(JobType jobType, String parametersJson, String requestedBy) {
        return JobRequest.builder()
                .jobId(UUID.randomUUID().toString())
                .jobType(jobType)
                .parametersJson(parametersJson)
                .requestedBy(requestedBy)
                .priority(1)
                .requestedAt(LocalDateTime.now())
                .build();
    }

    // Método helper para crear JobRequest con callback
    public static JobRequest withCallback(JobType jobType, String parametersJson,
                                          String callbackUrl) {
        return JobRequest.builder()
                .jobId(UUID.randomUUID().toString())
                .jobType(jobType)
                .parametersJson(parametersJson)
                .callbackUrl(callbackUrl)
                .requestedAt(LocalDateTime.now())
                .priority(2)
                .build();
    }

    // Método para obtener parámetro específico del mapa
    public Object getParameter(String key) {
        return parameters != null ? parameters.get(key) : null;
    }

    // Método para obtener parámetro específico con valor por defecto
    public Object getParameter(String key, Object defaultValue) {
        return parameters != null ? parameters.getOrDefault(key, defaultValue) : defaultValue;
    }

    // Método para convertir JSON a mapa si es necesario
    public Map<String, Object> getParametersAsMap() {
        if (parameters != null) {
            return parameters;
        }
        if (parametersJson != null && !parametersJson.isEmpty()) {
            // Aquí podrías usar Jackson ObjectMapper para parsear el JSON
            //return objectMapper.readValue(parametersJson, Map.class);
        }
        return Map.of();
    }

    // Validación básica del request
    public boolean isValid() {
        return jobId != null && !jobId.trim().isEmpty();
    }
}

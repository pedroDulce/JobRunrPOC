package org.example.batch.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.batch.dto.JobRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class JobRequestDeserializer extends JsonDeserializer<JobRequest> {

    private final ObjectMapper objectMapper;

    public JobRequestDeserializer() {
        super(JobRequest.class);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
    }

    @Override
    public JobRequest deserialize(String topic, Headers headers, byte[] data) {
        try {
            // Deserializar el payload
            JobRequest jobRequest = super.deserialize(topic, data);

            // Extraer headers y añadirlos al objeto
            extractAndAddHeaders(jobRequest, headers);

            // Validar mensaje
            validateJobRequest(jobRequest);

            log.debug("Successfully deserialized JobRequest: {}", jobRequest.getJobId());
            return jobRequest;

        } catch (Exception e) {
            log.error("Failed to deserialize JobRequest from topic {}: {}",
                    topic, e.getMessage());
            throw new RuntimeException("Deserialization failed", e);
        }
    }

    /**
     * Extrae headers de Kafka y los añade al JobRequest
     */
    private void extractAndAddHeaders(JobRequest jobRequest, Headers headers) {
        Map<String, String> extractedHeaders = new HashMap<>();

        headers.forEach(header -> {
            String key = header.key();
            String value = new String(header.value());
            extractedHeaders.put(key, value);

            // Setear valores importantes en el objeto
            switch (key) {
                case "job-type":
                    jobRequest.setJobType(value);
                    break;
                case "priority":
                    jobRequest.setPriority(value);
                    break;
                case "correlation-id":
                    jobRequest.setCorrelationId(value);
                    break;
                case "source":
                    jobRequest.setSource(value);
                    break;
            }
        });

        // Guardar todos los headers en metadata
        if (jobRequest.getMetadata() == null) {
            jobRequest.setMetadata(new HashMap<>());
        }
        jobRequest.getMetadata().put("kafkaHeaders", extractedHeaders);
    }

    /**
     * Validaciones básicas del JobRequest
     */
    private void validateJobRequest(JobRequest jobRequest) {
        if (jobRequest.getJobId() == null || jobRequest.getJobId().isEmpty()) {
            throw new IllegalArgumentException("Job ID is required");
        }

        if (jobRequest.getJobName() == null || jobRequest.getJobName().isEmpty()) {
            throw new IllegalArgumentException("Job name is required");
        }

        if (jobRequest.getScheduledAt() == null) {
            throw new IllegalArgumentException("Scheduled at is required");
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        // Configuración adicional si es necesaria
        this.addTrustedPackages("com.company.batchscheduler.model",
                "com.company.jobexecutor.model");
    }
}

package org.example.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class BatchJobExecutorService {

    private final JobLauncher jobLauncher;
    private final Job dailyTransactionBatchJob;
    private final BatchStatusNotifier batchStatusNotifier;

    /**
     * Escucha eventos de job-request-topic y ejecuta el batch correspondiente
     */
    @KafkaListener(topics = "job-request-topic", groupId = "job-executor-group")
    public void executeBatchJob(@Payload Map<String, Object> payload,
                                @Headers Map<String, Object> headers) {

        log.info("📨 Recibido evento para ejecución batch: {}", payload);

        // Verificar si es un batch job
        String targetBatch = (String) headers.get("target-batch");
        if (!"springbatch-1".equals(targetBatch)) {
            log.debug("Ignorando evento, target-batch no coincide: {}", targetBatch);
            return;
        }

        try {
            // Extraer información del payload
            String jobId = (String) payload.get("jobId");
            String jobName = (String) payload.get("jobName");
            Map<String, Object> parameters = (Map<String, Object>) payload.get("parameters");

            if (jobId == null) {
                log.error("❌ JobId no proporcionado en el payload");
                return;
            }

            log.info("🚀 Iniciando batch job: {} para jobId: {}", jobName, jobId);

            // Ejecutar el job de Spring Batch
            executeSpringBatchJob(jobId, jobName, parameters);

        } catch (Exception e) {
            log.error("💥 Error procesando evento batch: {}", e.getMessage(), e);
        }
    }

    /**
     * Ejecuta el job de Spring Batch
     */
    private void executeSpringBatchJob(String externalJobId, String jobName,
                                       Map<String, Object> parameters) {
        try {
            // Construir parámetros del job
            JobParametersBuilder paramsBuilder = new JobParametersBuilder()
                    .addString("externalJobId", externalJobId)
                    .addString("jobName", jobName)
                    .addString("executionTime", LocalDateTime.now().toString())
                    .addLong("timestamp", System.currentTimeMillis(), true);

            // Agregar parámetros adicionales
            if (parameters != null) {
                parameters.forEach((key, value) -> {
                    if (value instanceof String) {
                        paramsBuilder.addString(key, (String) value);
                    } else if (value instanceof Long) {
                        paramsBuilder.addLong(key, (Long) value);
                    } else if (value instanceof Integer) {
                        paramsBuilder.addLong(key, ((Integer) value).longValue());
                    } else {
                        paramsBuilder.addString(key, value.toString());
                    }
                });
            }

            JobParameters jobParameters = paramsBuilder.toJobParameters();

            // Ejecutar el job
            JobExecution execution = jobLauncher.run(dailyTransactionBatchJob, jobParameters);

            log.info("✅ Batch job lanzado. Execution ID: {}, Status: {}",
                    execution.getId(), execution.getStatus());

        } catch (Exception e) {
            log.error("❌ Error ejecutando batch job para {}: {}", externalJobId, e.getMessage(), e);

            // Notificar error al Job Scheduler
            batchStatusNotifier.notifyCompletion(
                    externalJobId,
                    "FAILED",
                    "Error iniciando batch: " + e.getMessage(),
                    null
            );
        }
    }
}

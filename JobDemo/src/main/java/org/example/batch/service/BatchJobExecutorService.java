package org.example.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class BatchJobExecutorService {

    private final JobLauncher jobLauncher;
    private final Job dailyTransactionBatchJob;
    private final BatchStatusNotifier batchStatusNotifier;


    /**
     * Ejecuta el job de Spring Batch
     */
    public void executeSpringBatchJob(String externalJobId, String jobName, Map<String, String> parameters) {
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
                    paramsBuilder.addString(key, value);
                });
            }

            JobParameters jobParameters = paramsBuilder.toJobParameters();

            // Ejecutar el job
            JobExecution execution = jobLauncher.run(dailyTransactionBatchJob, jobParameters);

            log.info("✅ Batch job lanzado. Execution ID: {}, Status: {}", execution.getId(), execution.getStatus());

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

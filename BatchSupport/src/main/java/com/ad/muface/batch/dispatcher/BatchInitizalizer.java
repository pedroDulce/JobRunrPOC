package com.ad.muface.batch.dispatcher;

import com.ad.muface.batch.dto.JobRequest;
import com.ad.muface.batch.dto.JobStatusEnum;
import com.ad.muface.batch.notifier.BatchDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@Slf4j
public class BatchInitizalizer extends BatchDispatcher {

    @Autowired
    private Job dailyTransactionBatchJob;

    protected void lanzarBatch(JobRequest jobRequest, Acknowledgment acknowledgment) {

        // Construir parámetros del job
        JobParametersBuilder paramsBuilder = new JobParametersBuilder()
                .addString("externalJobId", jobRequest.getJobId())
                .addString("jobName", jobRequest.getJobName())
                .addString("executionTime", LocalDateTime.now().toString())
                .addLong("timestamp", System.currentTimeMillis(), true);

        JobParameters jobParameters = paramsBuilder.toJobParameters();

        try {
            // 1. Publicar estado IN_PROGRESS
            kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.IN_PROGRESS, null,
                    "JobExecutor: remote Batch execution started");

            // 2. Confirmar offset
            acknowledgment.acknowledge();

            // 3. Ejecutar el batch
            JobExecution execution = jobLauncher.run(dailyTransactionBatchJob, jobParameters);

            log.info("✅ Batch job lanzado {}. Execution ID: {}, Status: {}", jobRequest.getJobName(),
                    execution.getId(), execution.getStatus());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing Batch request: {}", e.getMessage(), e);
            // Notificar error al Job Scheduler
            notifierProgress.notifyCompletion(
                    jobRequest.getJobId(),
                    "FAILED",
                    "Error iniciando batch: " + e.getMessage(),
                    null,
                    jobParameters.getLong("timestamp")
            );
            // Publicar estado FAILED si hay jobRequest
            if (jobRequest != null) {
                kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.FAILED, e,
                        "Batch execution failed: " + e.getMessage());
            }
            // No confirmar para que se reintente
        }
    }

}

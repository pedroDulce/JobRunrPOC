package com.ad.muface.jobs.demobatch.dispatcher;

import com.ad.muface.jobs.demobatch.job.CustomerSummaryReportJob;
import com.ad.muface.jobs.infra.notifier.BatchDispatcher;
import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
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

    @Autowired
    private CustomerSummaryReportJob jobExecutionService;


    protected void lanzarJob(JobRequest jobRequest, Acknowledgment acknowledgment) {
        try {

            // 1. Publicar estado IN_PROGRESS
            kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.IN_PROGRESS, null,
                    "JobExecutor: remote Job execution started");
            // Confirmar offset
            acknowledgment.acknowledge();

            // 2. Ejecutar el job
            JobResult result = jobExecutionService.executeJob(jobRequest);

            // 3. Publicar resultado final
            kafkaPublisher.publishJobResult(result);

            // 4. Confirmar offset
            acknowledgment.acknowledge();

            log.info("✅ JobExecutor: Job {} executed successfully", jobRequest.getJobId());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing job request: {}", e.getMessage(), e);

            kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.FAILED, e,
                    "Job execution failed: " + e.getMessage());
            // No confirmar para que se reintente la publicación
        }
    }

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

            log.info("✅ Batch job lanzado. Execution ID: {}, Status: {}", execution.getId(), execution.getStatus());

            log.info("✅ JobExecutor: Batch {} executed successfully", jobRequest.getJobName());

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

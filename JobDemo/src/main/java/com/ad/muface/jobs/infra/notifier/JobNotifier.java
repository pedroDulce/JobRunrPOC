package com.ad.muface.jobs.infra.notifier;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.ad.muface.jobs.demobatch.job.CustomerSummaryReportJob;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobNotifier {

    private final JobLauncher jobLauncher;
    private final Job dailyTransactionBatchJob;
    private final KafkaPublisher kafkaPublisher;
    private final NotifierProgress notifierProgress;
    private final CustomerSummaryReportJob jobExecutionService;

    @KafkaListener(
            topics = "${kafka.topics.job-requests}",
            containerFactory = "jobRequestListenerContainerFactory",
            groupId = "${spring.kafka.consumer.group-id}",
            id = "job-request-consumer"
    )
    @Transactional
    public void consumeJobRequest(
            ConsumerRecord<String, JobRequest> record,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = "business-domain", required = true) String businessDomain,
            @Header(value = "target-job", required = false) String targetJob,
            @Header(value = "target-batch", required = false) String targetBatch,
            @Header(value = "priority", defaultValue = "MEDIUM") String priority,
            @Header(value = "correlation-id", required = true) String correlationId,
            @Header(value = "jobrunr-job-id", required = true) String jobrunrJobId,
            Acknowledgment acknowledgment) {

        JobRequest jobRequest = record.value();

        log.info("""
                📥 JobExecutor: Received Job Request:
                Job ID: {}
                JobRunr Job ID: {}
                Business Domain: {}
                Target Job: {}
                Target Batch: {}
                Priority: {}
                Correlation ID: {}
                """,
                jobRequest.getJobId(),
                jobrunrJobId,
                businessDomain,
                targetJob,
                targetBatch,
                priority,
                correlationId
        );
        if (jobRequest.getJobId() == null) {
            jobRequest.setJobId(jobrunrJobId);
        }
        if (jobRequest.getCorrelationId() == null) {
            jobRequest.setCorrelationId(correlationId);
        }

        if (targetBatch != null) {
            lanzarBatch(jobRequest, acknowledgment);
        } else if (targetBatch != null) {
            lanzarJob(jobRequest, acknowledgment);
        }
    }

    private void lanzarJob(JobRequest jobRequest, Acknowledgment acknowledgment) {
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

    private void lanzarBatch(JobRequest jobRequest, Acknowledgment acknowledgment) {

        try {
            // 1. Publicar estado IN_PROGRESS
            kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.IN_PROGRESS, null,
                    "JobExecutor: remote Batch execution started");

            // 2. Confirmar offset
            acknowledgment.acknowledge();

            // 3. Ejecutar el batch
            executeSpringBatchJob(jobRequest.getJobId(), jobRequest.getJobName(),
                    jobRequest.getParameters());

            log.info("✅ JobExecutor: Batch {} executed successfully", jobRequest.getJobId());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing Batch request: {}", e.getMessage(), e);

            // Publicar estado FAILED si hay jobRequest
            if (jobRequest != null) {
                kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.FAILED, e,
                        "Batch execution failed: " + e.getMessage());
            }
            // No confirmar para que se reintente
        }
    }

    /**
     * Ejecuta el job de Spring Batch
     */
    private void executeSpringBatchJob(String externalJobId, String jobName, Map<String, String> parameters) {
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
            notifierProgress.notifyCompletion(
                    externalJobId,
                    "FAILED",
                    "Error iniciando batch: " + e.getMessage(),
                    null
            );
        }
    }


    /**
     * Extraer headers
     */
    private Map<String, String> logHeaders(ConsumerRecord<String, JobRequest> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> {
            headers.put(header.key(), new String(header.value()));
            log.debug("header.key: " + header.key() + " , header.value: " + header.value());
        });
        return headers;
    }
}

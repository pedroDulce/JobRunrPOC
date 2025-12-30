package org.example.batch.notifier;

import common.batch.dto.JobRequest;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.weaver.ast.Not;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchStatusConsumerAndNotifier {

    private final JobLauncher jobLauncher;
    private final Job dailyTransactionBatchJob;
    private final KafkaPublisher kafkaPublisher;
    private final NotifierProgress notifierProgress;

    @Transactional
    public void consumeBatchRequest(
            ConsumerRecord<String, JobRequest> record,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = "business-domain", required = true) String businessDomain,
            @Header(value = "target-batch", required = true) String targetBatch,
            @Header(value = "priority", defaultValue = "MEDIUM") String priority,
            @Header(value = "correlation-id", required = true) String correlationId,
            @Header(value = "jobrunr-job-id", required = true) String jobrunrJobId,
            @Payload Map<String, Object> payload,
            Acknowledgment acknowledgment) {

        try {
            JobRequest jobRequest = record.value();

            log.info("""
                    📥 JobExecutor: Received Batch Request:
                    Job ID: {}
                    JobRunr Job ID: {}
                    Business Domain: {}
                    Target Batch: {}
                    Priority: {}
                    Correlation ID: {}
                    """,
                    jobRequest.getJobId(),
                    jobrunrJobId,
                    businessDomain,
                    targetBatch,
                    priority,
                    correlationId
            );

            // 1. Publicar estado IN_PROGRESS
            kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.IN_PROGRESS, null,
                    correlationId, jobrunrJobId, "JobExecutor: remote Batch execution started");

            // 2. Ejecutar el batch
            executeSpringBatchJob(jobrunrJobId, jobRequest.getJobName(),
                    jobRequest.getParameters());

            // 4. Confirmar offset
            acknowledgment.acknowledge();

            log.info("✅ JobExecutor: Batch {} executed successfully", jobRequest.getJobId());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing Batch request: {}", e.getMessage(), e);

            // Publicar estado FAILED si hay jobRequest
            if (record != null && record.value() != null) {
                JobRequest jobRequest = record.value();
                kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.FAILED, e,
                        correlationId, jobrunrJobId, "Batch execution failed: " + e.getMessage());
            }

            // No confirmar para que se reintente
        }
    }

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
            notifierProgress.notifyCompletion(
                    externalJobId,
                    "FAILED",
                    "Error iniciando batch: " + e.getMessage(),
                    null
            );
        }
    }

}

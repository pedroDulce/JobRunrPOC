package com.ad.muface.batch.notifier;

import com.ad.muface.batch.dto.JobRequest;
import com.ad.muface.batch.service.HeartbeatService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
public abstract class BatchDispatcher {

    @Autowired
    protected JobLauncher jobLauncher;
    @Autowired
    protected KafkaPublisher notifierProgress;
    @Autowired
    protected HeartbeatService heartbeatService;

    @KafkaListener(
            topics = "${kafka.topics.job-requests}",
            containerFactory = "jobRequestListenerContainerFactory",
            groupId = "${spring.kafka.consumer.group-id}",
            id = "job-request-consumer"
    )
    public void consumeJobRequest(
            ConsumerRecord<String, JobRequest> record,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = "business-domain", required = true) String businessDomain,
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
        if (jobRequest.getJobId() == null) {
            jobRequest.setJobId(jobrunrJobId);
        }
        if (jobRequest.getCorrelationId() == null) {
            jobRequest.setCorrelationId(correlationId);
        }

        if (targetBatch != null) {
            launch(jobRequest, acknowledgment);
        }
    }

    private void launch(JobRequest jobRequest, Acknowledgment acknowledgment) {
        try {
            heartbeatService.startHeartbeat(jobRequest);
            lanzarBatch(jobRequest, acknowledgment);
        } catch (Exception e) {
            throw e;
        } finally {
            heartbeatService.stopHeartbeat(jobRequest.getJobRunnerId());
        }
    }

    protected void lanzarBatch(JobRequest jobRequest, Acknowledgment acknowledgment) {
        // Construir parámetros del job
        JobParametersBuilder paramsBuilder = new JobParametersBuilder()
                .addString("externalJobId", jobRequest.getJobId())
                .addString("jobName", jobRequest.getJobName())
                .addString("executionTime", LocalDateTime.now().toString())
                .addLong("timestamp", System.currentTimeMillis(), true);

        logJobRequestParameters(jobRequest.getParameters());

        JobParameters jobParameters = paramsBuilder.toJobParameters();

        try {

            // Ejecutar el batch
            JobExecution execution = jobLauncher.run(getJobToExecute(), jobParameters);

            log.info("✅ Batch job lanzado {}. Execution ID: {}, Status: {}", jobRequest.getJobName(),
                    execution.getId(), execution.getStatus());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing Batch request: {}", e.getMessage(), e);

            // Notificar error al Job Scheduler
            notifierProgress.notifyFailure(
                    jobRequest.getJobId(),
                    jobRequest.getJobName(),
                    jobRequest.getCorrelationId(),
                    "Error iniciando batch causado por " + e.getMessage(), null);

            // No confirmar para que se reintente
        }
    }

    protected abstract Job getJobToExecute();


    /**
     * Tracing de los headers
     */
    protected void logJobRequestHeaders(ConsumerRecord<String, JobRequest> record) {
        record.headers().forEach(header -> {
            log.debug("header.key: " + header.key() + " , header.value: " + header.value());
        });
    }


    /**
     * Tracing de los jobRequest parameters
     */
    protected void logJobRequestParameters(Map<String, String> jobRequestParameters) {
        jobRequestParameters.entrySet().forEach(param -> {
            log.debug("param.key:: " + param.getKey() + " - param.value:: " + param.getValue());
        });

    }

}

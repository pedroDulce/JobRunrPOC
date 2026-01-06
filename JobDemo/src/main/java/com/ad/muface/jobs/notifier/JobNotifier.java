package com.ad.muface.jobs.notifier;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.ad.muface.jobs.demobatch.job.CustomerSummaryReportJob;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobNotifier {

    private final CustomerSummaryReportJob jobExecutionService;
    private final KafkaPublisher kafkaPublisher;

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
            @Header(value = "target-job", required = true) String targetJob,
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
                Priority: {}
                Correlation ID: {}
                """,
                jobRequest.getJobId(),
                jobrunrJobId,
                businessDomain,
                targetJob,
                priority,
                correlationId
        );
        if (jobRequest.getJobId() == null) {
            jobRequest.setJobId(jobrunrJobId);
        }
        if (jobRequest.getCorrelationId() == null) {
            jobRequest.setCorrelationId(correlationId);
        }
        try {

            // 1. Publicar estado IN_PROGRESS
            kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.IN_PROGRESS, null,
                    "JobExecutor: remote Job execution started");
            // Confirmar offset
            acknowledgment.acknowledge();

            // 2. Ejecutar el job
            JobResult result = jobExecutionService.executeJob(jobRequest);

            // 3. Publicar resultado final
            kafkaPublisher.publishJobResult(result, correlationId, jobrunrJobId);

            // 4. Confirmar offset
            acknowledgment.acknowledge();

            log.info("✅ JobExecutor: Job {} executed successfully", jobRequest.getJobId());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing job request: {}", e.getMessage(), e);

            // Publicar estado FAILED si hay jobRequest
            if (record != null && record.value() != null) {
                jobRequest = record.value();
                kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.FAILED, e,
                        "Job execution failed: " + e.getMessage());
            }
            // No confirmar para que se reintente la publicación
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

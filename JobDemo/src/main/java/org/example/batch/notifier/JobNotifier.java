package org.example.batch.notifier;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.batch.job.CustomerSummaryReportJob;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
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

        JobResult result;
        try {
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

            // 1. Publicar estado IN_PROGRESS
            kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.IN_PROGRESS, null,
                    correlationId, jobrunrJobId, "JobExecutor: remote Job execution started");
            // Confirmar offset
            acknowledgment.acknowledge();

            // 2. Ejecutar el job
            result = jobExecutionService.executeJob(jobRequest, extractHeaders(record));

            // 3. Publicar resultado final
            kafkaPublisher.publishJobResult(result, correlationId, jobrunrJobId);

            // 4. Confirmar offset
            acknowledgment.acknowledge();

            log.info("✅ JobExecutor: Job {} executed successfully", jobRequest.getJobId());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing job request: {}", e.getMessage(), e);

            // Publicar estado FAILED si hay jobRequest
            if (record != null && record.value() != null) {
                JobRequest jobRequest = record.value();
                kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.FAILED, e,
                        correlationId, jobrunrJobId, "Job execution failed: " + e.getMessage());
            }

            // No confirmar para que se reintente
        }
    }



    /**
     * Extraer headers
     */
    private Map<String, String> extractHeaders(ConsumerRecord<String, JobRequest> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> {
            headers.put(header.key(), new String(header.value()));
        });
        return headers;
    }
}

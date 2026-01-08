package com.ad.muface.batch.notifier;

import com.ad.muface.batch.dto.JobRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class BatchDispatcher {

    @Autowired
    protected JobLauncher jobLauncher;
    @Autowired
    protected KafkaPublisher kafkaPublisher;
    @Autowired
    protected NotifierProgress notifierProgress;

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
            lanzarBatch(jobRequest, acknowledgment);
        }
    }

    protected abstract void lanzarBatch(JobRequest jobRequest, Acknowledgment acknowledgment);


    /**
     * Extraer headers
     */
    protected Map<String, String> logHeaders(ConsumerRecord<String, JobRequest> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> {
            headers.put(header.key(), new String(header.value()));
            log.debug("header.key: " + header.key() + " , header.value: " + header.value());
        });
        return headers;
    }

}

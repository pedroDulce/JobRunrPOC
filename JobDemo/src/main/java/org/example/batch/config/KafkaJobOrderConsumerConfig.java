package org.example.batch.config;

import common.batch.dto.JobRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.*;

@Slf4j
@Configuration
@EnableKafka
public class KafkaJobOrderConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:job-executor-group}")
    private String groupId;

    @Value("${kafka.consumer.concurrency:1}")
    private int concurrency;

    @Value("${kafka.filter.enabled:true}")
    private boolean filterEnabled;

    @Value("${kafka.filter.business-domains:}")
    private String businessDomainsConfig;

    @Value("${kafka.filter.target-batches:}")
    private String targetBatchesConfig;

    @Value("${kafka.filter.target-jobs:}")
    private String targetJobsConfig;

    private Set<String> allowedBusinessDomains;
    private Set<String> allowedTargetBatches;
    private Set<String> allowedTargetJobs;

    @Autowired(required = false)
    private JobRecordInterceptor jobRecordInterceptor;

    /**
     * Inicializar sets de filtrado
     */
    private void initializeFilterSets() {
        allowedBusinessDomains = parseCommaSeparatedSet(businessDomainsConfig);
        allowedTargetBatches = parseCommaSeparatedSet(targetBatchesConfig);
        allowedTargetJobs = parseCommaSeparatedSet(targetJobsConfig);

        log.info("Kafka Filter Configuration:");
        log.info("  Enabled: {}", filterEnabled);
        log.info("  Allowed Business Domains: {}", allowedBusinessDomains);
        log.info("  Allowed Target Batches: {}", allowedTargetBatches);
        log.info("  Allowed Target Jobs: {}", allowedTargetJobs);
    }

    /**
     * Parsear valores separados por comas
     */
    private Set<String> parseCommaSeparatedSet(String config) {
        Set<String> result = new HashSet<>();
        if (config != null && !config.trim().isEmpty()) {
            Arrays.stream(config.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .forEach(result::add);
        }
        return result;
    }

    /**
     * Consumer Factory para JobRequest (JSON)
     */
    @Bean
    public ConsumerFactory<String, JobRequest> simpleJobRequestConsumerFactory() {
        initializeFilterSets(); // Inicializar filtros

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);

        // Configuración directa para JsonDeserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, JobRequest.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Container Factory principal para JobRequest
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, JobRequest>
    jobRequestListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, JobRequest> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(simpleJobRequestConsumerFactory());
        factory.setConcurrency(concurrency);

        // Configurar acknowledgement manual
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Configurar filtrado por headers
        factory.setRecordFilterStrategy(record -> {
            return shouldFilterMessage(record);
        });

        // Configurar error handler
        factory.setCommonErrorHandler(simpleErrorHandler());

        // Configurar interceptor si existe
        if (jobRecordInterceptor != null) {
            factory.setRecordInterceptor(jobRecordInterceptor);
            log.info("JobRecordInterceptor registered successfully");
        }

        return factory;
    }

    /**
     * Lógica de filtrado basada en business-domain y target-batch
     */
    private boolean shouldFilterMessage(ConsumerRecord<String, JobRequest> record) {
        // Si el filtrado está deshabilitado, procesar todos los mensajes
        if (!filterEnabled) {
            return false;
        }

        try {
            String businessDomain = extractHeader(record, "business-domain");
            String targetBatch = extractHeader(record, "target-batch");
            String targetJob = extractHeader(record, "target-job");

            log.debug("Checking message - BusinessDomain: {}, TargetBatch: {}, Key: {}",
                    businessDomain, targetBatch, record.key());

            // Verificar business-domain
            boolean businessDomainMatches = allowedBusinessDomains.isEmpty() ||
                    (businessDomain != null && allowedBusinessDomains.contains(businessDomain));

            // Verificar target-batch
            boolean targetBatchMatches = allowedTargetBatches.isEmpty() ||
                    (targetBatch != null && allowedTargetBatches.contains(targetBatch));

            // Verificar target-batch
            boolean targetJobMatches = allowedTargetJobs.isEmpty() ||
                    (targetBatch != null && allowedTargetJobs.contains(targetJob));

            // Si ambos criterios coinciden, NO filtrar (procesar)
            boolean shouldProcess = businessDomainMatches && targetBatchMatches;

            if (!shouldProcess) {
                log.debug("Filtering message - Key: {}, BusinessDomain: {}, TargetBatch: {}",
                        record.key(), businessDomain, targetBatch);

                // Notificar al interceptor si existe
                if (jobRecordInterceptor != null) {
                    String reason = String.format("BusinessDomain: %s, TargetBatch: %s",
                            businessDomain, targetBatch);
                    jobRecordInterceptor.onFiltered(record, reason);
                }
            }

            return !shouldProcess;

        } catch (Exception e) {
            log.error("Error in message filtering for key {}: {}",
                    record.key(), e.getMessage());
            return true; // Filtrar en caso de error
        }
    }

    /**
     * Extraer valor de header
     */
    private String extractHeader(ConsumerRecord<?, ?> record, String headerName) {
        try {
            org.apache.kafka.common.header.Header header =
                    record.headers().lastHeader(headerName);
            return header != null ? new String(header.value()) : null;
        } catch (Exception e) {
            log.debug("Error extracting header {}: {}", headerName, e.getMessage());
            return null;
        }
    }

    /**
     * Error Handler simple
     */
    @Bean
    public CommonErrorHandler simpleErrorHandler() {
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 3);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(fixedBackOff);

        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                org.springframework.messaging.converter.MessageConversionException.class,
                org.springframework.kafka.support.serializer.DeserializationException.class
        );

        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("Failed delivery for job {}: attempt {}", record.key(), deliveryAttempt);
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.info("Job {} recovered after exception: {}", record.key(), ex.getMessage());
            }
        });

        return errorHandler;
    }

    /**
     * Bean para KafkaTemplate genérico
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JsonSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "1");

        ProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }
}

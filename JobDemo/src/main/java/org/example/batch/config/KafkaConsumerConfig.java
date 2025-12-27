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
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:job-executor-group}")
    private String groupId;

    @Value("${kafka.consumer.concurrency:1}")
    private int concurrency;

    @Value("${spring.application.name:job-executor-service}")
    private String serviceName;

    @Autowired(required = false)
    private JobRecordInterceptor jobRecordInterceptor;

    /**
     * Consumer Factory para JobRequest (JSON)
     */
    @Bean
    public ConsumerFactory<String, JobRequest> jobRequestConsumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);

        // Deserializadores con ErrorHandlingDeserializer
        ErrorHandlingDeserializer<String> keyDeserializer =
                new ErrorHandlingDeserializer<>(new StringDeserializer());

        // JsonDeserializer para JobRequest
        JsonDeserializer<JobRequest> valueDeserializer = new JsonDeserializer<>(JobRequest.class);
        valueDeserializer.addTrustedPackages("*");
        valueDeserializer.setUseTypeHeaders(false);

        ErrorHandlingDeserializer<JobRequest> errorHandlingValueDeserializer =
                new ErrorHandlingDeserializer<>(valueDeserializer);

        return new DefaultKafkaConsumerFactory<>(
                props,
                keyDeserializer,
                errorHandlingValueDeserializer
        );
    }

    /**
     * Consumer Factory alternativa (más simple)
     */
    @Bean
    public ConsumerFactory<String, JobRequest> simpleJobRequestConsumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

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
            return !shouldProcessMessage(record);
        });

        // Configurar error handler SIMPLE (sin DLQ por ahora)
        factory.setCommonErrorHandler(simpleErrorHandler());

        // Configurar interceptor si existe
        if (jobRecordInterceptor != null) {
            factory.setRecordInterceptor(jobRecordInterceptor);
        }

        return factory;
    }

    /**
     * Error Handler simple (sin DeadLetterPublishingRecoverer)
     */
    @Bean
    public CommonErrorHandler simpleErrorHandler() {
        // Backoff: 1 segundo, 3 intentos
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 3);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(fixedBackOff);

        // No reintentar para estas excepciones
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                org.springframework.messaging.converter.MessageConversionException.class,
                org.springframework.kafka.support.serializer.DeserializationException.class
        );

        // Listener para logs de reintentos
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
     * Lógica de filtrado basada en headers
     */
    private boolean shouldProcessMessage(ConsumerRecord<String, JobRequest> record) {
        try {
            // 1. Verificar header 'target-service'
            String targetService = getHeaderValue(record, "target-service");
            if (targetService != null && !targetService.equalsIgnoreCase(serviceName)) {
                log.debug("Ignoring message: target-service {} doesn't match {}",
                        targetService, serviceName);
                return false;
            }

            // 2. Verificar que el job type sea soportado
            String jobType = getHeaderValue(record, "job-type");
            if (jobType != null && !isJobTypeSupported(jobType)) {
                log.debug("Ignoring message: job-type {} not supported", jobType);
                return false;
            }

            return true;

        } catch (Exception e) {
            log.error("Error in message filtering: {}", e.getMessage());
            return false;
        }
    }

    private String getHeaderValue(ConsumerRecord<?, ?> record, String headerName) {
        try {
            org.apache.kafka.common.header.Header header = record.headers().lastHeader(headerName);
            return header != null ? new String(header.value()) : null;
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isJobTypeSupported(String jobType) {
        List<String> supportedJobTypes = getSupportedJobTypes();
        return jobType != null && supportedJobTypes.contains(jobType.toUpperCase());
    }

    protected List<String> getSupportedJobTypes() {
        return Arrays.asList(
                "CUSTOMER_SUMMARY",
                "CUSTOMER_EXPORT",
                "REPORT_GENERATION",
                "DATA_SYNC",
                "RESUMEN_DIARIO_CLIENTES"  // Añade los tipos que usas
        );
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

package org.example.batch.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
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

    @Value("${kafka.consumer.concurrency:3}")
    private int concurrency;

    @Value("${spring.application.name:unknown-service}")
    private String serviceName;

    @Autowired(required = false)
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private JobRecordInterceptor jobRecordInterceptor;

    /**
     * Consumer Factory con deserialización segura
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Deserializadores con manejo de errores
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class);

        // Configurar los deserializadores reales
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS,
                StringDeserializer.class.getName());
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                JobRequestDeserializer.class.getName());

        // Paquetes confiables para deserialización
        //props.put(ErrorHandlingDeserializer.TRUSTED_PACKAGES, "*");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Container Factory para jobs con filtrado por headers
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object>
    jobRequestListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(concurrency);

        // Configurar acknowledgement manual
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Configurar filtrado por headers
        factory.setRecordFilterStrategy(new RecordFilterStrategy<String, Object>() {
            @Override
            public boolean filter(ConsumerRecord<String, Object> record) {
                boolean shouldFilter = !shouldProcessMessage(record);
                if (shouldFilter) {
                    jobRecordInterceptor.onFiltered(record, "Filtered by header rules");
                }
                return shouldFilter;
            }
        });

        // Configurar error handler
        factory.setCommonErrorHandler(kafkaErrorHandler());

        // Configurar interceptor para logs - ¡CORREGIDO!
        factory.setRecordInterceptor(jobRecordInterceptor);

        // Opcional: Configurar listener para batch processing
        factory.setBatchListener(false); // false para records individuales

        return factory;
    }

    /**
     * Error Handler con Dead Letter Queue
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> {
                    log.error("Sending to DLQ: topic={}, partition={}, offset={}, exception={}",
                            record.topic(), record.partition(), record.offset(), exception.getMessage());

                    // Determinar topic DLQ basado en el topic original
                    return new TopicPartition(record.topic() + "-dlq", record.partition());
                }
        );

        // Configurar backoff fijo con 3 reintentos
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 3);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, fixedBackOff);

        // No reintentar para ciertas excepciones
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                org.springframework.messaging.converter.MessageConversionException.class
        );

        // Configurar función para determinar si reintentar
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("Failed delivery for record {}: attempt {}", record.key(), deliveryAttempt);
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.info("Record {} recovered after exception: {}", record.key(), ex.getMessage());
            }
        });

        return errorHandler;
    }

    /**
     * Lógica de filtrado basada en headers
     */
    private boolean shouldProcessMessage(ConsumerRecord<String, Object> record) {
        try {
            // 1. Verificar header 'target-service'
            String targetService = getHeaderValue(record, "target-service");
            if (targetService != null && !targetService.equalsIgnoreCase(serviceName)) {
                log.debug("Ignoring message: target-service {} doesn't match {}",
                        targetService, serviceName);
                return false;
            }

            // 2. Verificar header 'job-category' (si está configurado)
            String jobCategory = getHeaderValue(record, "job-category");
            if (jobCategory != null && !isCategorySupported(jobCategory)) {
                log.debug("Ignoring message: job-category {} not supported", jobCategory);
                return false;
            }

            // 3. Verificar header 'business-domain' (opcional)
            String businessDomain = getHeaderValue(record, "business-domain");
            if (businessDomain != null && !isDomainSupported(businessDomain)) {
                log.debug("Ignoring message: business-domain {} not supported", businessDomain);
                return false;
            }

            // 4. Verificar TTL (Time To Live)
            String ttlHeader = getHeaderValue(record, "time-to-live");
            if (ttlHeader != null && isExpired(ttlHeader)) {
                log.warn("Ignoring expired message: TTL {}", ttlHeader);
                return false;
            }

            // 5. Verificar que el job type sea soportado
            String jobType = getHeaderValue(record, "job-type");
            if (jobType != null && !isJobTypeSupported(jobType)) {
                log.debug("Ignoring message: job-type {} not supported", jobType);
                return false;
            }

            return true;

        } catch (Exception e) {
            log.error("Error in message filtering: {}", e.getMessage());
            return false; // Por seguridad, ignorar si hay error en filtrado
        }
    }

    private String getHeaderValue(ConsumerRecord<?, ?> record, String headerName) {
        try {
            org.apache.kafka.common.header.Header header = record.headers().lastHeader(headerName);
            return header != null ? new String(header.value()) : null;
        } catch (Exception e) {
            log.debug("Error reading header {}: {}", headerName, e.getMessage());
            return null;
        }
    }

    private boolean isCategorySupported(String category) {
        // Definir categorías soportadas por este servicio
        List<String> supportedCategories = getSupportedCategories();
        return supportedCategories.contains(category.toUpperCase());
    }

    private boolean isDomainSupported(String domain) {
        // Definir dominios soportados
        List<String> supportedDomains = getSupportedDomains();
        return supportedDomains.contains(domain.toUpperCase());
    }

    private boolean isJobTypeSupported(String jobType) {
        // Definir job types soportados
        List<String> supportedJobTypes = getSupportedJobTypes();
        return supportedJobTypes.contains(jobType.toUpperCase());
    }

    private boolean isExpired(String ttlHeader) {
        try {
            // Asumiendo formato ISO 8601: "2024-01-15T23:59:00"
            java.time.LocalDateTime ttl = java.time.LocalDateTime.parse(ttlHeader);
            return java.time.LocalDateTime.now().isAfter(ttl);
        } catch (Exception e) {
            log.debug("Invalid TTL format: {}", ttlHeader);
            return false;
        }
    }

    /**
     * Métodos para obtener configuraciones de filtrado (pueden ser sobrescritos)
     */
    protected List<String> getSupportedCategories() {
        return Arrays.asList("CUSTOMER", "REPORTING", "BILLING", "MAINTENANCE");
    }

    protected List<String> getSupportedDomains() {
        return Arrays.asList("SALES", "FINANCE", "OPERATIONS", "MARKETING");
    }

    protected List<String> getSupportedJobTypes() {
        return Arrays.asList(
                "CUSTOMER_SUMMARY",
                "CUSTOMER_EXPORT",
                "REPORT_GENERATION",
                "DATA_SYNC"
        );
    }

    /**
     * Factory para consumidores de alta prioridad
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object>
    highPriorityListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(1); // Menos concurrencia para alta prioridad

        // Filtrar solo mensajes de alta prioridad
        factory.setRecordFilterStrategy(record -> {
            String priority = getHeaderValue(record, "priority");
            return !"HIGH".equalsIgnoreCase(priority);
        });

        return factory;
    }

    /**
     * Factory para consumidores de reprocesamiento (DLQ)
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object>
    dlqListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(1);

        // Configurar para procesar mensajes de DLQ
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(1000);

        return factory;
    }

    /**
     * Bean para inicializar KafkaTemplate si no existe
     */
    @Bean
    @ConditionalOnMissingBean(KafkaTemplate.class)
    public KafkaTemplate<String, Object> defaultKafkaTemplate() {
        log.warn("Creating default KafkaTemplate - consider configuring a proper one");

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JsonSerializer.class);

        ProducerFactory<String, Object> producerFactory =
                new DefaultKafkaProducerFactory<>(props);

        return new KafkaTemplate<>(producerFactory);
    }
}
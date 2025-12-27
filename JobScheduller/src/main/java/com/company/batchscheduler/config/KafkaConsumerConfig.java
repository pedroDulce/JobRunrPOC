package com.company.batchscheduler.config;

import common.batch.dto.JobResult;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, JobResult> kafkaListenerContainerFactory(
            ConsumerFactory<String, JobResult> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, JobResult> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Configurar reintentos
        /*factory.setCommonErrorHandler(new SeekToCurrentErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate()),
                3 // Número de reintentos
        ));*/

        return factory;
    }
}

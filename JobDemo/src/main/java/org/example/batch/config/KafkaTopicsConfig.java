package org.example.batch.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Value("${kafka.topics.job-requests}")
    private String jobRequestsTopic;

    @Value("${kafka.topics.job-results}")
    private String jobResultsTopic;

    @Bean
    public NewTopic jobRequestsTopic() {
        return TopicBuilder.name(jobRequestsTopic)
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic jobResultsTopic() {
        return TopicBuilder.name(jobResultsTopic)
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }
}

package com.company.batchscheduler.config;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

@Component
public class JobHeaderInterceptor implements ProducerInterceptor<String, Object> {

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
        // Añadir timestamp automático si no existe
        if (record.headers().lastHeader("producer-timestamp") == null) {
            record.headers().add("producer-timestamp",
                    String.valueOf(Instant.now().toEpochMilli()).getBytes());
        }

        // Añadir source automático si no existe
        if (record.headers().lastHeader("source") == null) {
            record.headers().add("source", "batch-scheduler-service".getBytes());
        }

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // Lógica post-envío (opcional)
    }

    @Override
    public void close() {
        // Cleanup
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Configuración
    }
}

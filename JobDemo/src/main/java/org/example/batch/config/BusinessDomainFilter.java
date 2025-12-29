package org.example.batch.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.stereotype.Component;

@Component
public class BusinessDomainFilter implements RecordFilterStrategy<String, String> {

    private static final String BUSINESS_DOMAIN_HEADER = "business-domain";
    private static final String TARGET_DOMAIN = "My-critical-service";

    @Override
    public boolean filter(ConsumerRecord<String, String> consumerRecord) {
        // Filtrar mensajes que NO tengan el business-domain deseado
        Iterable<Header> headers = consumerRecord.headers();
        for (Header header : headers) {
            if (BUSINESS_DOMAIN_HEADER.equals(header.key())) {
                String value = new String(header.value());
                return !TARGET_DOMAIN.equals(value);
            }
        }
        // Si no tiene el header, también lo filtramos
        return true;
    }
}

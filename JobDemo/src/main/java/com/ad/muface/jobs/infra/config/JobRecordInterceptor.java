package com.ad.muface.jobs.infra.config;

import common.batch.dto.JobRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
public class JobRecordInterceptor implements RecordInterceptor<String, JobRequest> {

    private Instant lastProcessedTime;
    private int totalRecordsProcessed = 0;
    private int totalRecordsFiltered = 0;
    private Instant lastBatchStart;

    /**
     * Método 1: Interceptar antes de procesar el record
     * Este es el método requerido por la interfaz
     */
    public ConsumerRecord<String, JobRequest> intercept(ConsumerRecord<String, JobRequest> record) {
        Instant start = Instant.now();

        // Log tiempo desde último procesamiento
        if (lastProcessedTime != null) {
            Duration timeSinceLast = Duration.between(lastProcessedTime, start);
            if (timeSinceLast.toMillis() > 10000) { // Más de 10 segundos
                log.debug("Long pause detected: {} ms since last record",
                        timeSinceLast.toMillis());
            }
        }

        lastProcessedTime = start;

        // Log básico del record
        log.trace("📥 Intercepting record - Key: {}, Topic: {}, Partition: {}, Offset: {}",
                record.key(), record.topic(), record.partition(), record.offset());

        // Extraemos y logging de headers
        record.headers().forEach(header -> {
            log.debug("   Header {}: {}", header.key(), new String(header.value()));
        });

        return record;
    }

    /**
     * Método 2: Interceptar con acceso al Consumer (opcional)
     */
    @Override
    public ConsumerRecord<String, JobRequest> intercept(ConsumerRecord<String, JobRequest> record,
                                                    Consumer<String, JobRequest> consumer) {
        // Por defecto, delegamos al método sin Consumer
        return intercept(record);
    }

    /**
     * Método 3: Llamado cuando el procesamiento es exitoso
     */
    public void success(ConsumerRecord<String, JobRequest> record, Object result) {
        totalRecordsProcessed++;

        log.debug("✅ Successfully processed record - Key: {}, Offset: {}",
                record.key(), record.offset());

        // Log cada 100 records procesados
        if (totalRecordsProcessed % 100 == 0) {
            log.info("📊 Total records processed: {}", totalRecordsProcessed);
        }
    }

    /**
     * Método 4: Llamado cuando el procesamiento falla
     */
    public void failure(ConsumerRecord<String, Object> record, Exception exception) {
        log.error("❌ Failed to process record - Key: {}, Topic: {}, Partition: {}, Offset: {}, Error: {}",
                record.key(), record.topic(), record.partition(), record.offset(),
                exception.getMessage());

        // Puedes agregar lógica adicional aquí, como métricas específicas
    }

    /**
     * Método 5: Llamado después del record (opcional)
     */
    public void afterRecord(ConsumerRecord<String, JobRequest> record, JobRequest result) {
        // Cleanup o post-processing opcional
        // Por ejemplo, limpiar recursos temporales
    }

    /**
     * Método 6: Llamado cuando un record es filtrado (no es parte de la interfaz estándar)
     */
    // En JobRecordInterceptor, agrega este método si no existe:
    public void onFiltered(ConsumerRecord<String, JobRequest> record, String reason) {
        totalRecordsFiltered++;

        log.debug("🚫 Record filtered - Key: {}, Reason: {}", record.key(), reason);

        // Log cada 10 records filtrados
        if (totalRecordsFiltered % 10 == 0) {
            log.info("🚫 Total records filtered: {}", totalRecordsFiltered);
        }
    }

    /**
     * Método para el inicio de un batch de records
     */
    public void onBatchStart() {
        lastBatchStart = Instant.now();
        log.debug("🔄 Starting batch processing at {}", lastBatchStart);
    }

    /**
     * Método para el fin de un batch de records
     */
    public void onBatchComplete(int batchSize, long processingTimeMs) {
        if (lastBatchStart != null) {
            Duration batchDuration = Duration.between(lastBatchStart, Instant.now());
            log.debug("🏁 Batch completed - Size: {}, Duration: {} ms",
                    batchSize, batchDuration.toMillis());
        }
    }

    /**
     * Métodos para métricas
     */
    public int getTotalRecordsProcessed() {
        return totalRecordsProcessed;
    }

    public int getTotalRecordsFiltered() {
        return totalRecordsFiltered;
    }

    public void resetCounters() {
        totalRecordsProcessed = 0;
        totalRecordsFiltered = 0;
        lastBatchStart = null;
        lastProcessedTime = null;
    }

    /**
     * Obtener estadísticas de procesamiento
     */
    public ProcessingStats getStats() {
        return ProcessingStats.builder()
                .totalRecordsProcessed(totalRecordsProcessed)
                .totalRecordsFiltered(totalRecordsFiltered)
                .lastProcessedTime(lastProcessedTime)
                .build();
    }

    /**
     * Clase para estadísticas
     */
    @lombok.Builder
    @lombok.Data
    public static class ProcessingStats {
        private int totalRecordsProcessed;
        private int totalRecordsFiltered;
        private Instant lastProcessedTime;
        private Instant lastBatchStart;
        private Instant lastBatchEnd;

        public long getRecordsPerMinute() {
            if (lastProcessedTime == null) return 0;

            Duration runtime = Duration.between(lastBatchStart != null ? lastBatchStart : Instant.now().
                    minusSeconds(3600),                   Instant.now());
            if (runtime.toMinutes() == 0) return totalRecordsProcessed;

            return totalRecordsProcessed / runtime.toMinutes();
        }
    }
}

package org.example.batch.service;

import common.batch.dto.JobStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@Data
@RequiredArgsConstructor
public class JobMetricsService {

    private final MeterRegistry meterRegistry;

    @Value("${spring.application.name:unknown-service}")
    private String serviceName;

    @Value("${spring.profiles.active:default}")
    private String activeProfile;

    /**
     * Registrar recepción de un job
     */
    public void recordJobReceived(String jobId, String jobType, String priority) {
        Counter counter = Counter.builder("kafka.job.received")
                .description("Número de jobs recibidos desde Kafka")
                .tags(getCommonTags())
                .tag("job_type", jobType != null ? jobType : "unknown")
                .tag("priority", priority != null ? priority : "MEDIUM")
                .tag("job_id", jobId != null ? jobId : "unknown")
                .register(meterRegistry);

        counter.increment();

        // También incrementar contador total
        Counter.builder("kafka.job.total.received")
                .tags(getCommonTags())
                .register(meterRegistry)
                .increment();
    }

    /**
     * Registrar finalización de un job
     */
    public void recordJobCompleted(String jobId, JobStatus status) {
        Counter.builder("kafka.job.completed")
                .description("Jobs completados exitosamente")
                .tags(getCommonTags())
                .tag("status", status != null ? status.toString() : "unknown")
                .tag("job_id", jobId != null ? jobId : "unknown")
                .register(meterRegistry)
                .increment();
    }

    /**
     * Registrar fallo en procesamiento de job
     */
    public void recordJobFailed(String jobId, String errorMessage) {
        Counter.builder("kafka.job.failed")
                .description("Jobs que fallaron en procesamiento")
                .tags(getCommonTags())
                .tag("job_id", jobId != null ? jobId : "unknown")
                .tag("error_type", extractErrorType(errorMessage))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Registrar tiempo de procesamiento
     */
    public void recordProcessingTime(String jobId, long durationMs) {
        Timer timer = Timer.builder("kafka.job.processing.time")
                .description("Tiempo de procesamiento de jobs")
                .tags(getCommonTags())
                .tag("job_id", jobId != null ? jobId : "unknown")
                .register(meterRegistry);

        timer.record(durationMs, TimeUnit.MILLISECONDS);

        // También registrar como gauge para ver el último valor
        meterRegistry.gauge("kafka.job.last.processing.time.ms",
                Arrays.asList(
                        Tag.of("service", serviceName),
                        Tag.of("job_id", jobId != null ? jobId : "unknown")
                ),
                durationMs);
    }

    /**
     * Registrar lag del consumer
     */
    public void recordConsumerLag(String topic, int partition, long lag) {
        meterRegistry.gauge("kafka.consumer.lag",
                Arrays.asList(
                        Tag.of("service", serviceName),
                        Tag.of("topic", topic != null ? topic : "unknown"),
                        Tag.of("partition", String.valueOf(partition)),
                        Tag.of("profile", activeProfile)
                ),
                lag);
    }

    /**
     * Registrar publicación de resultado exitosa
     */
    public void recordResultPublished(String jobId) {
        Counter.builder("kafka.job.result.published")
                .description("Resultados de jobs publicados exitosamente")
                .tags(getCommonTags())
                .tag("job_id", jobId != null ? jobId : "unknown")
                .register(meterRegistry)
                .increment();
    }

    /**
     * Registrar fallo en publicación de resultado
     */
    public void recordResultPublishFailed(String jobId) {
        Counter.builder("kafka.job.result.publish.failed")
                .description("Fallos en publicación de resultados")
                .tags(getCommonTags())
                .tag("job_id", jobId != null ? jobId : "unknown")
                .register(meterRegistry)
                .increment();
    }

    /**
     * Registrar tiempo de respuesta del consumer
     */
    public void recordConsumerPollTime(long pollTimeMs) {
        Timer.builder("kafka.consumer.poll.time")
                .description("Tiempo de poll del consumer")
                .tags(getCommonTags())
                .register(meterRegistry)
                .record(pollTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Registrar número de registros procesados por poll
     */
    public void recordRecordsPerPoll(int recordCount) {
        meterRegistry.gauge("kafka.consumer.records.per.poll",
                getCommonTags(),
                recordCount);
    }

    /**
     * Obtener tags comunes para todas las métricas
     */
    private List<Tag> getCommonTags() {
        return Arrays.asList(
                Tag.of("service", serviceName),
                Tag.of("profile", activeProfile),
                Tag.of("instance", getInstanceId()),
                Tag.of("version", getApplicationVersion())
        );
    }

    /**
     * Obtener ID de instancia (para escalado horizontal)
     */
    private String getInstanceId() {
        try {
            return System.getenv("HOSTNAME") != null ?
                    System.getenv("HOSTNAME") :
                    "local-" + System.currentTimeMillis();
        } catch (Exception e) {
            return "unknown-instance";
        }
    }

    /**
     * Obtener versión de la aplicación
     */
    private String getApplicationVersion() {
        try {
            Package pkg = getClass().getPackage();
            return pkg != null && pkg.getImplementationVersion() != null ?
                    pkg.getImplementationVersion() :
                    "1.0.0";
        } catch (Exception e) {
            return "unknown-version";
        }
    }

    /**
     * Extraer tipo de error del mensaje
     */
    private String extractErrorType(String errorMessage) {
        if (errorMessage == null) {
            return "unknown";
        }

        if (errorMessage.contains("Timeout")) {
            return "timeout";
        } else if (errorMessage.contains("Connection") || errorMessage.contains("Network")) {
            return "network";
        } else if (errorMessage.contains("Validation") || errorMessage.contains("Invalid")) {
            return "validation";
        } else if (errorMessage.contains("Database") || errorMessage.contains("SQL")) {
            return "database";
        } else if (errorMessage.contains("Memory") || errorMessage.contains("Heap")) {
            return "memory";
        } else {
            return "business";
        }
    }

    /**
     * Método para crear tags dinámicamente
     */
    public List<Tag> createTags(String... tags) {
        if (tags.length % 2 != 0) {
            throw new IllegalArgumentException("Tags must be in key-value pairs");
        }

        Tag[] tagArray = new Tag[tags.length / 2];
        for (int i = 0; i < tags.length; i += 2) {
            tagArray[i / 2] = Tag.of(tags[i], tags[i + 1]);
        }

        return Arrays.asList(tagArray);
    }

    /**
     * Método para registrar métricas personalizadas
     */
    public void recordCustomMetric(String metricName, double value, String... tags) {
        meterRegistry.gauge(metricName, createTags(tags), value);
    }

    /**
     * Método para incrementar contador personalizado
     */
    public void incrementCounter(String counterName, String... tags) {
        Counter.builder(counterName)
                .tags(createTags(tags))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Método para registrar timer personalizado
     */
    public void recordTimer(String timerName, long duration, TimeUnit unit, String... tags) {
        Timer.builder(timerName)
                .tags(createTags(tags))
                .register(meterRegistry)
                .record(duration, unit);
    }
}

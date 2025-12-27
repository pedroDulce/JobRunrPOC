package org.example.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class JobMetricsService {

    // Métricas en memoria si no tienes Micrometer
    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

    public void recordJobReceived(String jobId, String jobType, String priority) {
        incrementCounter("job.received", jobType, priority);
        log.info("📊 Job received - ID: {}, Type: {}, Priority: {}", jobId, jobType, priority);
    }

    public void recordJobCompleted(String jobId, String status) {
        incrementCounter("job.completed", status);
        log.info("📊 Job completed - ID: {}, Status: {}", jobId, status);
    }

    public void recordJobFailed(String jobId, String errorMessage) {
        incrementCounter("job.failed", extractErrorType(errorMessage));
        log.error("📊 Job failed - ID: {}, Error: {}", jobId, errorMessage);
    }

    public void recordProcessingTime(String jobId, long durationMs) {
        log.debug("📊 Processing time - ID: {}, Duration: {}ms", jobId, durationMs);
    }

    public void recordResultPublished(String jobId) {
        incrementCounter("result.published");
        log.info("📊 Result published - ID: {}", jobId);
    }

    public void recordResultPublishFailed(String jobId) {
        incrementCounter("result.publish.failed");
        log.error("📊 Result publish failed - ID: {}", jobId);
    }

    private void incrementCounter(String metricName, String... tags) {
        String key = buildKey(metricName, tags);
        counters.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    private String buildKey(String metricName, String... tags) {
        if (tags.length == 0) {
            return metricName;
        }
        return metricName + ":" + String.join(".", tags);
    }

    private String extractErrorType(String errorMessage) {
        if (errorMessage == null) return "unknown";

        if (errorMessage.contains("Timeout")) return "timeout";
        if (errorMessage.contains("Connection") || errorMessage.contains("Network")) return "network";
        if (errorMessage.contains("Validation") || errorMessage.contains("Invalid")) return "validation";
        if (errorMessage.contains("Database") || errorMessage.contains("SQL")) return "database";

        return "business";
    }

    // Método para obtener métricas (útil para debugging)
    public void logMetrics() {
        log.info("=== Current Metrics ===");
        counters.forEach((key, value) -> {
            log.info("{}: {}", key, value.get());
        });
        log.info("======================");
    }

    // Método para resetear métricas (útil para testing)
    public void resetMetrics() {
        counters.clear();
        log.info("Metrics reset");
    }
}

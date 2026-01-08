package com.ad.muface.batch.service;

import com.ad.muface.batch.dto.JobResult;
import com.ad.muface.batch.dto.JobStatusEnum;
import com.ad.muface.batch.notifier.KafkaPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class HeartbeatService implements ApplicationListener<ContextClosedEvent> {
    private final KafkaPublisher kafkaPublisher;
    private final ScheduledExecutorService sharedExecutor;
    private final Map<String, ScheduledFuture<?>> heartbeatTasks;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public HeartbeatService(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
        this.heartbeatTasks = new ConcurrentHashMap<>();
        this.sharedExecutor = Executors.newScheduledThreadPool(
                2, // Pool pequeño - solo para heartbeats
                r -> {
                    Thread t = new Thread(r, "heartbeat-" + Calendar.getInstance().getTime());
                    t.setDaemon(true);
                    return t;
                }
        );
    }

    public void startHeartbeat(String jobId, String jobName, String correlationId) {

        if (shuttingDown.get()) {
            throw new IllegalStateException("Service is shutting down");
        }

        ScheduledFuture<?> future = sharedExecutor.scheduleAtFixedRate(
                () -> sendHeartbeat(jobId, jobName, correlationId),
                0, 5, TimeUnit.SECONDS
        );

        heartbeatTasks.put(jobId, future);

        // Enviar heartbeat inmediatamente
        sendHeartbeat(jobId, jobName, correlationId);
    }

    public void stopHeartbeat(String jobId) {
        ScheduledFuture<?> task = heartbeatTasks.remove(jobId);
        if (task != null) {
            task.cancel(false);
        }
    }

    private void sendHeartbeat(String jobId, String jobName, String correlationId) {
        try {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("instanceId", jobId);
            metadata.put("timestamp", System.currentTimeMillis());
            metadata.put("thread", Thread.currentThread().getName());

            JobResult heartbeat = new JobResult();
            heartbeat.setJobId(jobId);
            heartbeat.setJobrunrJobId(jobId);
            heartbeat.setLastHeartBeat(LocalDateTime.now());
            heartbeat.setStatus(JobStatusEnum.IN_PROGRESS);
            heartbeat.setMetadata(metadata);

            kafkaPublisher.notifyProgress(jobId, jobName, correlationId,"job en ejecución...", heartbeat);

        } catch (Exception e) {
            log.warn("Error creating heartbeat", e);
        }
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        shuttingDown.set(true);

        // Enviar heartbeats de shutdown para todos los servicios
        heartbeatTasks.keySet().forEach(this::stopHeartbeat);

        // Apagar executor con timeout
        sharedExecutor.shutdown();
        try {
            if (!sharedExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                sharedExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            sharedExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // Método para verificar health check
    public boolean isHealthy() {
        return !shuttingDown.get() && !sharedExecutor.isShutdown();
    }


}

package com.ad.muface.jobs.infra;

import com.ad.muface.jobs.infra.notifier.KafkaPublisher;
import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class HeartbeatService implements ApplicationListener<ContextClosedEvent> {
    private final KafkaPublisher kafkaPublisher;
    private final InstanceIdentifier instanceIdentifier;
    private final ScheduledExecutorService sharedExecutor;
    private final Map<String, ScheduledFuture<?>> heartbeatTasks;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public HeartbeatService(KafkaPublisher kafkaPublisher, InstanceIdentifier instanceIdentifier) {
        this.kafkaPublisher = kafkaPublisher;
        this.instanceIdentifier = instanceIdentifier;
        this.heartbeatTasks = new ConcurrentHashMap<>();

        this.sharedExecutor = Executors.newScheduledThreadPool(
                2, // Pool pequeño - solo para heartbeats
                r -> {
                    Thread t = new Thread(r, "heartbeat-" + instanceIdentifier.getInstanceId());
                    t.setDaemon(true);
                    return t;
                }
        );
    }

    public void startHeartbeat(JobRequest jobRequest) {
        String fulljobId = jobRequest.getJobId() + "-" + instanceIdentifier.getInstanceId();
        jobRequest.setJobRunnerId(fulljobId);

        if (shuttingDown.get()) {
            throw new IllegalStateException("Service is shutting down");
        }

        ScheduledFuture<?> future = sharedExecutor.scheduleAtFixedRate(
                () -> sendHeartbeat(fulljobId, jobRequest.getJobId(), jobRequest),
                0, jobRequest.getHeartBeatLapse(), TimeUnit.SECONDS
        );

        heartbeatTasks.put(fulljobId, future);

        // Enviar heartbeat inmediatamente
        sendHeartbeat(fulljobId, jobRequest.getJobId(), jobRequest);
    }

    public void stopHeartbeat(String fulljobId) {
        ScheduledFuture<?> task = heartbeatTasks.remove(fulljobId);
        if (task != null) {
            task.cancel(false);
        }
    }

    private void sendHeartbeat(String fulljobId, String serviceType, JobRequest jobRequest) {
        try {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("instanceId", fulljobId);
            metadata.put("serviceType", serviceType);
            metadata.put("timestamp", System.currentTimeMillis());
            metadata.put("thread", Thread.currentThread().getName());

            if (jobRequest != null && jobRequest.getMetadata() != null) {
                metadata.putAll(jobRequest.getMetadata());
            }

            JobResult heartbeat = new JobResult();
            heartbeat.setJobId(fulljobId);
            heartbeat.setLastHeartBeat(LocalDateTime.now());
            heartbeat.setStatus(JobStatusEnum.IN_PROGRESS);
            heartbeat.setMetadata(metadata);

            kafkaPublisher.publishJobHeartBeat(fulljobId, heartbeat);

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

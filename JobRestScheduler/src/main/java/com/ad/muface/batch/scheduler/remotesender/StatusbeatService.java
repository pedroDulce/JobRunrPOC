package com.ad.muface.batch.scheduler.remotesender;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@Slf4j
public class StatusbeatService {
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);
    private final Map<String, ScheduledFuture<?>> heartbeatTasks = new ConcurrentHashMap<>();
    private final AtomicInteger activeHeartbeats = new AtomicInteger(0);

    /**
     * Inicia el heartbeat para un job
     */
    public void startHeartbeat(String jobId, Runnable heartbeatTask, long initialDelay, long period, TimeUnit unit) {
        synchronized (this) {
            log.info("🚀 INICIANDO HEARTBEAT para job: {}", jobId);

            // 1. Si ya existe un heartbeat para este jobId, primero lo detenemos
            stopHeartbeat(jobId);

            // 2. Crear nueva tarea de heartbeat
            ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(() -> {
                try {
                    heartbeatTask.run();
                } catch (Exception e) {
                    log.error("❌ Error ejecutando heartbeat para job: {}", jobId, e);
                }
            }, initialDelay, period, unit);

            // 3. Guardar la tarea en el mapa
            heartbeatTasks.put(jobId, task);
            activeHeartbeats.incrementAndGet();

            log.info("✅ Heartbeat iniciado para job: {}. Heartbeats activos: {}",
                    jobId, activeHeartbeats.get());
            log.debug("Tareas activas: {}", heartbeatTasks.keySet());
        }
    }

    public void stopHeartbeat(String taskId) {
        ScheduledFuture<?> task = heartbeatTasks.remove(taskId);
        if (task != null) {
            task.cancel(false); // false = permite que la tarea en curso termine
            activeHeartbeats.decrementAndGet();
            log.info("🛑 Heartbeat detenido para: {}", taskId);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Apagando HeartBeatService...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

}

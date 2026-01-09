package com.ad.muface.batch.service;

import com.ad.muface.batch.dto.JobResult;
import com.ad.muface.batch.dto.JobStatusEnum;
import com.ad.muface.batch.notifier.KafkaPublisher;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@Slf4j
public class HeartbeatService {

    private final KafkaPublisher kafkaPublisher;
    private final Map<String, ScheduledFuture<?>> heartbeatTasks = new ConcurrentHashMap<>();

    // Single executor service para toda la aplicación
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // Contador de heartbeats activos para debugging
    private final AtomicInteger activeHeartbeats = new AtomicInteger(0);

    /**
     * Inicia el heartbeat para un job
     */
    public void startHeartbeat(String jobId, String jobName, String correlationId) {
        synchronized (this) {
            log.info("🚀 INICIANDO HEARTBEAT para job: {} (Nombre: {}, Correlation: {})",
                    jobId, jobName, correlationId);

            // 1. Si ya existe un heartbeat para este jobId, primero lo detenemos
            stopHeartbeat(jobId);

            // 2. Crear nueva tarea de heartbeat
            ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(() -> {
                try {
                    sendHeartbeat(jobId, jobName, correlationId);
                } catch (Exception e) {
                    log.error("❌ Error enviando heartbeat para job: {}", jobId, e);
                }
            }, 0, 5, TimeUnit.SECONDS); // Cada 5 segundos

            // 3. Guardar la tarea en el mapa
            heartbeatTasks.put(jobId, task);
            activeHeartbeats.incrementAndGet();

            log.info("✅ Heartbeat iniciado para job: {}. Heartbeats activos: {}",
                    jobId, activeHeartbeats.get());
            log.debug("Tareas activas: {}", heartbeatTasks.keySet());
        }
    }

    /**
     * Detiene el heartbeat para un job
     */
    public void stopHeartbeat(String jobId) {
        synchronized (this) {
            log.info("🛑 DETENIENDO HEARTBEAT para job: {}", jobId);

            ScheduledFuture<?> task = heartbeatTasks.remove(jobId);
            if (task != null) {
                try {
                    // Cancelar la tarea (true para interrumpir si está en ejecución)
                    boolean cancelled = task.cancel(true);

                    if (cancelled) {
                        activeHeartbeats.decrementAndGet();
                        log.info("✅ Heartbeat detenido exitosamente para job: {}. Heartbeats activos: {}",
                                jobId, activeHeartbeats.get());
                    } else {
                        log.warn("⚠️ No se pudo cancelar el heartbeat para job: {}", jobId);
                    }
                } catch (Exception e) {
                    log.error("❌ Error deteniendo heartbeat para job: {}", jobId, e);
                }
            } else {
                log.warn("⚠️ No se encontró heartbeat activo para job: {}", jobId);
            }

            log.debug("Tareas restantes después de detener {}: {}", jobId, heartbeatTasks.keySet());
        }
    }

    /**
     * Método para enviar el heartbeat (implementación real)
     */
    private void sendHeartbeat(String jobId, String jobName, String correlationId) {
        LocalDateTime now = LocalDateTime.now();
        String threadName = Thread.currentThread().getName();

        log.info("❤️ HEARTBEAT - Job: {}, Thread: {}, Time: {}",
                jobId, threadName, now);

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

        // Ejemplo de implementación con logging
        String heartbeatMessage = String.format(
                "Heartbeat for job %s (%s) - Correlation: %s - Time: %s - Thread: %s",
                jobId, jobName, correlationId, now, threadName
        );

        // Simulamos envío (reemplazar con tu implementación real)
        System.out.println("[HEARTBEAT] " + heartbeatMessage);
    }

    /**
     * Detiene TODOS los heartbeats (para shutdown de la aplicación)
     */
    public void stopAllHeartbeats() {
        synchronized (this) {
            log.info("🛑 DETENIENDO TODOS LOS HEARTBEATS. Total activos: {}", activeHeartbeats.get());

            // Crear copia de las keys para evitar ConcurrentModificationException
            String[] jobIds = heartbeatTasks.keySet().toArray(new String[0]);

            for (String jobId : jobIds) {
                stopHeartbeat(jobId);
            }

            log.info("✅ Todos los heartbeats detenidos. Heartbeats activos: {}", activeHeartbeats.get());
        }
    }

    /**
     * Verifica si hay un heartbeat activo para un job
     */
    public boolean isHeartbeatActive(String jobId) {
        return heartbeatTasks.containsKey(jobId);
    }

    /**
     * Obtiene estadísticas de heartbeats
     */
    public Map<String, Object> getHeartbeatStats() {
        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("activeHeartbeats", activeHeartbeats.get());
        stats.put("scheduledTasks", heartbeatTasks.size());
        stats.put("jobIds", new java.util.ArrayList<>(heartbeatTasks.keySet()));
        stats.put("timestamp", LocalDateTime.now());
        return stats;
    }

    /**
     * Limpieza al destruir el bean
     */
    @PreDestroy
    public void shutdown() {
        log.info("🔧 Apagando HeartbeatService...");

        // 1. Detener todos los heartbeats
        stopAllHeartbeats();

        // 2. Apagar el scheduler
        try {
            scheduler.shutdown();
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                log.warn("Scheduler forzado a shutdown");
            }
            log.info("✅ Scheduler apagado correctamente");
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
            log.error("❌ Error apagando scheduler", e);
        }
    }

}

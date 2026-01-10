package com.ad.muface.batch.scheduler.controller;

import com.ad.muface.batch.dto.JobRequest;
import com.ad.muface.batch.scheduler.remotesender.RemoteJobRestInvoker;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


@Tag(name = "Job Scheduling", description = "API para programación de trabajos batch")
@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
@Slf4j
public class JobSchedulerController {

    private final RemoteJobRestInvoker remoteJobRestInvoker;

    private final JobScheduler jobScheduler;

    @PostMapping("/inmediate-remote-sync")
    public ResponseEntity<Map<String, Object>> scheduleSyncInmediateJob(@RequestBody JobRequest request) {
        UUID jobId = UUID.randomUUID();
        request.setJobId(jobId.toString());

        // JobRunr puede serializar estos parámetros String correctamente
        jobScheduler.schedule(
                jobId,
                Instant.now(),
                () -> remoteJobRestInvoker.invocarJobRemoto(request, null)
        );

        Map<String, Object> response = new HashMap<>();
        response.put("jobId", jobId.toString());
        response.put("jobName", request.getJobName());
        response.put("jobUrlBaseDestino", request.getUrlMicroDestino());
        response.put("jobType", request.getJobType());
        response.put("business-domain", request.getBusinessDomain());
        response.put("status", "SCHEDULED");
        response.put("cronExpression", request.getCronExpression());
        response.put("processDate", request.getParameters().get("processDate"));
        response.put("message", "Job programado con éxito.");
        response.put("dashboardUrl", "http://localhost:8000");

        log.info("✅ Job de ejecución inmediata {}", jobId);

        return ResponseEntity.ok(response);
    }

    @PostMapping("/schedule-remote-sync")
    public ResponseEntity<Map<String, Object>> scheduleSyncviaRestRecurringJob(@RequestBody JobRequest request) {
        validateCronExpression(request.getCronExpression());

        String jobId = UUID.randomUUID().toString();
        request.setJobId(jobId);

        // JobRunr puede serializar estos parámetros String correctamente
        jobScheduler.scheduleRecurrently(
                request.getJobName(),
                request.getCronExpression(),
                () -> remoteJobRestInvoker.invocarJobRemoto(request, null)
        );
        Map<String, Object> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("jobName", request.getJobName());
        response.put("jobType", request.getJobType());
        response.put("business-domain", request.getBusinessDomain());
        response.put("status", "SCHEDULED");
        response.put("cronExpression", request.getCronExpression());
        response.put("processDate", request.getParameters().get("processDate"));
        response.put("message", "Job programado con éxito.");
        response.put("dashboardUrl", "http://localhost:8000");

        log.info("✅ Job programado: {} con cron: {}", jobId, request.getCronExpression());

        return ResponseEntity.ok(response);
    }

    private void validateCronExpression(String cronExpression) {
        if (cronExpression == null || cronExpression.trim().isEmpty()) {
            throw new IllegalArgumentException("La expresión cron no puede estar vacía");
        }

        String[] parts = cronExpression.split("\\s+");
        if (parts.length != 6) {
            throw new IllegalArgumentException(
                    "La expresión cron debe tener 6 campos (segundos minutos horas día mes día-semana). " +
                            "Ejemplo: 0 0 2 1 * *   ==> 1er día de cada mes a las 2 AM. " +
                            "Recibido: " + cronExpression
            );
        }
    }


}

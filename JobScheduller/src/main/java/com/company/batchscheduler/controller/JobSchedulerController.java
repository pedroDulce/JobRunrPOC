package com.company.batchscheduler.controller;

import com.company.batchscheduler.job.EmbebbedCustomerSummaryJob;
import com.company.batchscheduler.producer.KafkaPublisherForJobs;
import common.batch.dto.JobRequest;
import common.batch.dto.JobType;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


@Tag(name = "Job Scheduling", description = "API para programación de trabajos batch")
@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
@Slf4j
public class JobSchedulerController {

    private final KafkaPublisherForJobs kafkaPublisherWithoutHeadersInMessagesForJobs;

    private final JobScheduler jobScheduler;

    private final EmbebbedCustomerSummaryJob embebbedCustomerSummaryJob;

    private final RemoteJobDispatcher remoteJobDispatcher;

    @PostMapping("/schedule-remote-async")
    public ResponseEntity<Map<String, Object>> executeRemoteJob(@RequestBody JobRequest request) {
        validateCronExpression(request.getCronExpression());

        String jobId = UUID.randomUUID().toString();

        // Preparar parámetros como Strings
        String processDateStr = (request.getScheduledAt() != null)
                ? request.getScheduledAt().toString()
                : LocalDate.now().toString();

        // JobRunr puede serializar estos parámetros String correctamente
        jobScheduler.scheduleRecurrently(
                jobId,
                request.getCronExpression(),
                () -> kafkaPublisherWithoutHeadersInMessagesForJobs.publishEventForRunJob(jobId, request)
        );

        Map<String, Object> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("status", "SCHEDULED");
        response.put("cronExpression", request.getCronExpression());
        response.put("processDate", processDateStr);
        response.put("message", "Job programado exitosamente");
        response.put("dashboardUrl", "http://localhost:8000");

        log.info("✅ Job programado: {} con cron: {}", jobId, request.getCronExpression());

        return ResponseEntity.ok(response);

    }


    @PostMapping("/execute-now-and-once")
    @Operation(summary = "Ejecutar job inmediatamente")
    public ResponseEntity<Map<String, Object>> executeNow(
            @Valid @RequestBody JobRequest request) {

        try {
            String jobId = UUID.randomUUID().toString();

            // Preparar parámetros
            String processDateStr = (request.getScheduledAt() != null)
                    ? request.getScheduledAt().toString()
                    : LocalDate.now().minusDays(1).toString();

            String emailRecipient = request.getParameters().get("emailRecipient");

            jobScheduler.enqueue(() ->
                    embebbedCustomerSummaryJob.executeImmediately(
                            processDateStr,
                            emailRecipient
                    )
            );

            Map<String, Object> response = new HashMap<>();
            response.put("jobId", jobId);
            response.put("status", "ENQUEUED");
            response.put("executionTime", LocalDateTime.now());
            response.put("dashboardUrl", "http://localhost:8000");
            response.put("message", "Job encolado para ejecución inmediata");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error ejecutando job inmediato: {}", e.getMessage(), e);

            Map<String, Object> error = new HashMap<>();
            error.put("status", "ERROR");
            error.put("message", e.getMessage());

            return ResponseEntity.status(500).body(error);
        }
    }

    @PostMapping("/schedule-remote-sync")
    public ResponseEntity<?> scheduleRemoteJob(@RequestBody JobRequest request) {
        String jobId = UUID.randomUUID().toString();
        String microUrl = request.getParameters().get("url");

        jobScheduler.scheduleRecurrently(
                jobId,
                request.getCronExpression(),
                () -> remoteJobDispatcher.executeRestRemote(
                        jobId,
                        JobType.SYNCRONOUS.toString(),
                        microUrl,
                        request.getParameters()
                )
        );

        return ResponseEntity.ok(Map.of(
                "jobId", jobId,
                "status", "SCHEDULED",
                "microservice", "job-executor:" + microUrl,
                "jobType", JobType.SYNCRONOUS
        ));
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

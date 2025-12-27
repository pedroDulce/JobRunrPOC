package com.company.batchscheduler.controller;

import com.company.batchscheduler.job.EmbebbedCustomerSummaryJob;
import com.company.batchscheduler.job.KafkaPublisherForJobs;
import common.batch.dto.ImmediateJobRequest;
import common.batch.dto.JobRequest;
import common.batch.dto.JobType;
import common.batch.model.JobStatus;
import common.batch.repository.JobStatusRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


@Tag(name = "Job Scheduling", description = "API para programación de trabajos batch")
@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
@Slf4j
public class JobSchedulerController {

    private final JobStatusRepository statusRepository;

    private final KafkaPublisherForJobs kafkaPublisherForJobs;

    private final JobScheduler jobScheduler;
    private final EmbebbedCustomerSummaryJob embebbedCustomerSummaryJob;

    private final RemoteJobDispatcher remoteJobDispatcher;

    @PostMapping("/execute-remote-async")
    public ResponseEntity<Map<String, Object>> executeRemoteJob(@RequestBody JobRequest request) {
        validateCronExpression(request.getCronExpression());

        String jobId = UUID.randomUUID().toString();

        // Preparar parámetros como Strings
        String processDateStr = (request.getProcessDate() != null)
                ? request.getProcessDate().toString()
                : LocalDate.now().toString();

        // JobRunr puede serializar estos parámetros String correctamente
        jobScheduler.scheduleRecurrently(
                jobId,
                request.getCronExpression(),
                () -> kafkaPublisherForJobs.publishEventForRunJob(jobId, request)
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


    @GetMapping("/status/{jobId}")
    public ResponseEntity<JobStatus> getStatus(@PathVariable String jobId) {
        return statusRepository.findByJobId(jobId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/schedule-recurrent")
    @Operation(summary = "Programar job recurrente")
    public ResponseEntity<Map<String, Object>> scheduleRecurringJob(
            @Valid @RequestBody JobRequest request) {

        try {
            log.info("Recibida solicitud para programar job recurrente: {}", request);

            validateCronExpression(request.getCronExpression());

            String jobId = UUID.randomUUID().toString();

            // Preparar parámetros como Strings
            String processDateStr = (request.getProcessDate() != null)
                    ? request.getProcessDate().toString()
                    : LocalDate.now().toString();

            String emailRecipient = request.getMetadata().get("emailRecipient") != null
                    ? request.getMetadata().get("emailRecipient")
                    : "default@company.com";

            // JobRunr puede serializar estos parámetros String correctamente
            jobScheduler.scheduleRecurrently(
                    jobId,
                    request.getCronExpression(),
                    () -> embebbedCustomerSummaryJob.generateDailySummary(
                            jobId,           // String
                            processDateStr,  // String
                            emailRecipient   // String
                    )
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

        } catch (Exception e) {
            log.error("❌ Error programando job recurrente: {}", e.getMessage(), e);

            Map<String, Object> error = new HashMap<>();
            error.put("status", "ERROR");
            error.put("message", e.getMessage());
            error.put("error", e.getClass().getSimpleName());

            return ResponseEntity.status(500).body(error);
        }
    }

    @PostMapping("/execute-now")
    @Operation(summary = "Ejecutar job inmediatamente")
    public ResponseEntity<Map<String, Object>> executeNow(
            @Valid @RequestBody ImmediateJobRequest request) {

        try {
            String jobId = UUID.randomUUID().toString();

            // Preparar parámetros
            String processDateStr = (request.getProcessDate() != null)
                    ? request.getProcessDate().toString()
                    : LocalDate.now().minusDays(1).toString();

            String emailRecipient = request.getEmailRecipient() != null
                    ? request.getEmailRecipient()
                    : "default@company.com";

            jobScheduler.enqueue(() ->
                    embebbedCustomerSummaryJob.executeImmediately(
                            processDateStr,
                            request.isSendEmail(),
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

    @PostMapping("/schedule-once")
    @Operation(summary = "Programar job para ejecución única")
    public ResponseEntity<Map<String, Object>> scheduleSyncRemote(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.TIME) LocalTime time,
            @RequestParam(required = false) String email) {

        try {
            if (date == null || time == null) {
                throw new IllegalArgumentException("Los parámetros 'date' y 'time' son requeridos");
            }

            LocalDateTime scheduledTime = LocalDateTime.of(date, time);

            if (scheduledTime.isBefore(LocalDateTime.now())) {
                throw new IllegalArgumentException("La fecha/hora programada no puede ser en el pasado");
            }

            String jobId = UUID.randomUUID().toString();
            String processDateStr = date.toString();
            String emailRecipient = email != null ? email : "default@company.com";

            jobScheduler.schedule(
                    scheduledTime,
                    () -> embebbedCustomerSummaryJob.generateDailySummary(
                            jobId,
                            processDateStr,
                            emailRecipient
                    )
            );

            Map<String, Object> response = new HashMap<>();
            response.put("jobId", jobId);
            response.put("status", "SCHEDULED");
            response.put("scheduledTime", scheduledTime);
            response.put("message", "Job programado para ejecución única");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error programando ejecución única: {}", e.getMessage(), e);

            Map<String, Object> error = new HashMap<>();
            error.put("status", "ERROR");
            error.put("message", e.getMessage());
            error.put("error", e.getClass().getSimpleName());

            return ResponseEntity.status(400).body(error);
        }
    }

    @PostMapping("/schedule-remote-sync")
    public ResponseEntity<?> scheduleRemoteJob(@RequestBody JobRequest request) {
        String jobId = UUID.randomUUID().toString();
        String microUrl = (String) request.getParameter("url");

        jobScheduler.scheduleRecurrently(
                jobId,
                request.getCronExpression(),
                () -> remoteJobDispatcher.executeRestRemote(
                        jobId,
                        JobType.SYNCRONOUS,
                        microUrl,
                        request.getParametersJson()
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

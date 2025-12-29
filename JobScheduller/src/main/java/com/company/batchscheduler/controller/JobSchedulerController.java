package com.company.batchscheduler.controller;

import com.company.batchscheduler.sendnotifier.KafkaPublisherForJobs;
import common.batch.dto.JobRequest;
import common.batch.dto.JobType;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


@Tag(name = "Job Scheduling", description = "API para programación de trabajos batch")
@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
@Slf4j
public class JobSchedulerController {

    private final KafkaPublisherForJobs publisherForJobs;

    private final JobScheduler jobScheduler;

    private final RemoteJobDispatcher remoteJobDispatcher;

    @PostMapping("/schedule-remote-async")
    public ResponseEntity<Map<String, Object>> executeRemoteJob(@RequestBody JobRequest request) {
        validateCronExpression(request.getCronExpression());

        String jobId = UUID.randomUUID().toString();
        request.setJobId(jobId);

        // JobRunr puede serializar estos parámetros String correctamente
        jobScheduler.scheduleRecurrently(
                request.getJobName(),
                request.getCronExpression(),
                () -> publisherForJobs.publishEventForRunJob(request, null)
        );
        Map<String, Object> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("jobName", request.getJobName());
        response.put("business-domain", request.getBusinessDomain());
        response.put("status", "SCHEDULED");
        response.put("cronExpression", request.getCronExpression());
        response.put("processDate", request.getParameters().get("processDate"));
        response.put("message", "Job programado exitosamente");
        response.put("dashboardUrl", "http://localhost:8000");

        log.info("✅ Job programado: {} con cron: {}", jobId, request.getCronExpression());

        return ResponseEntity.ok(response);

    }


    @PostMapping("/schedule-remote-sync")
    public ResponseEntity<?> scheduleRemoteJob(@RequestBody JobRequest request) {
        String jobId = UUID.randomUUID().toString();
        request.setJobId(jobId);
        String microUrl = request.getParameters().get("url");

        jobScheduler.scheduleRecurrently(
                request.getJobName(),
                request.getCronExpression(),
                () -> remoteJobDispatcher.executeRestRemote(request, null)
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

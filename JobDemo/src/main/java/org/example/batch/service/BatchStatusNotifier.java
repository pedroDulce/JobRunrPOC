package org.example.batch.service;

import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.batch.notifier.JobNotifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchStatusNotifier {

    private final JobNotifier jobNotifier;

    @Value("${kafka.topics.job-results}")
    private String jobResultsTopic;

    /**
     * Notifica inicio del batch job
     */
    public void notifyStart(String jobId, String message) {

        JobResult statusResult = JobResult.builder()
                .jobId(jobId)
                .jobName("jobRequest.getJobName()")
                .status(JobStatusEnum.IN_PROGRESS)
                .message(message)
                .startedAt(LocalDateTime.now())
                .completedAt(null)
                .errorDetails(null)
                .correlationId("correlationId")
                .jobrunrJobId(jobId)  // IMPORTANTE: ID de JobRunr
                .build();

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        jobNotifier.publishToResultsTopic(statusResult);

        log.info("📤 JobExecutor: Published final result for job {} with status {}",
                statusResult.getJobId(), JobStatusEnum.IN_PROGRESS);

        log.info("📤 Notificado INICIO del batch job: {}", jobId);
    }


    /**
     * Notifica progreso del batch job
     */
    public void notifyProgress(String jobId, String message, int progress) {

        JobResult statusResult = JobResult.builder()
                .jobId(jobId)
                .jobName("jobRequest.getJobName()")
                .status(JobStatusEnum.IN_PROGRESS)
                .message(message)
                .startedAt(LocalDateTime.now())
                .completedAt(null)
                .errorDetails(null)
                .correlationId("correlationId")
                .jobrunrJobId(jobId)  // IMPORTANTE: ID de JobRunr
                .build();

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        jobNotifier.publishToResultsTopic(statusResult);

        log.debug("📤 Notificado PROGRESO del batch job {}: {}%", jobId, progress);
    }

    /**
     * Notifica finalización del batch job
     */
    public void notifyCompletion(String jobId, String status, String message,
                                 Map<String, Object> report) {

        JobResult statusResult = JobResult.builder()
                .jobId(jobId)
                .jobName("jobRequest.getJobName()")
                .status(JobStatusEnum.COMPLETED)
                .message(message)
                .startedAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .errorDetails(null)
                .correlationId("correlationId")
                .metadata(report != null ? report : Map.of("stage", "COMPLETED"))
                .jobrunrJobId(jobId)  // IMPORTANTE: ID de JobRunr
                .build();

        // Asegurar que tiene el jobrunrJobId
        if (jobId != null) {
            statusResult.setJobrunrJobId(jobId);
        }
        jobNotifier.publishToResultsTopic(statusResult);

        log.info("📤 Notificado COMPLETADO del batch job {}: {}", jobId, status);
    }

    /**
     * Envía email con el informe
     */
    public void sendEmailReport(String jobId, Map<String, Object> report) {
        try {
            // Aquí integrarías con tu servicio de email
            // Por ejemplo: JavaMailSender, SendGrid, Amazon SES, etc.

            String emailContent = buildEmailContent(jobId, report);
            log.info("📧 Email generado para job {}:\n{}", jobId, emailContent);

            // Ejemplo con JavaMailSender (descomentar y configurar):
            /*
            SimpleMailMessage message = new SimpleMailMessage();
            message.setTo("operations@empresa.com");
            message.setSubject("Informe Batch Job: " + jobId);
            message.setText(emailContent);
            mailSender.send(message);
            */

        } catch (Exception e) {
            log.warn("⚠️ No se pudo enviar email para job {}: {}", jobId, e.getMessage());
        }
    }

    private String buildEmailContent(String jobId, Map<String, Object> report) {
        return String.format("""
            =================================
            INFORME DE PROCESAMIENTO BATCH
            =================================
            Job ID: %s
            Fecha: %s
            ---------------------------------
            RESUMEN EJECUCIÓN:
            - Estado: %s
            - Duración: %s
            - Registros procesados: %s
            - Particiones ejecutadas: %s
            ---------------------------------
            DETALLES:
            %s
            =================================
            """,
                jobId,
                LocalDateTime.now(),
                report.get("status"),
                report.get("duration"),
                report.get("writeCount"),
                report.get("partitions"),
                formatReportDetails(report)
        );
    }

    private String formatReportDetails(Map<String, Object> report) {
        StringBuilder details = new StringBuilder();
        report.forEach((key, value) ->
                details.append(String.format("- %s: %s%n", key, value))
        );
        return details.toString();
    }
}

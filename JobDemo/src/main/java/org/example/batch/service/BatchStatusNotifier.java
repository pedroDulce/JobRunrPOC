package org.example.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchStatusNotifier {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String JOB_RESULTS_TOPIC = "job-results-topic";

    /**
     * Notifica inicio del batch job
     */
    public void notifyStart(String jobId, String message) {
        Map<String, Object> result = new HashMap<>();
        result.put("jobId", jobId);
        result.put("status", "PROGRESS");
        result.put("message", message);
        result.put("progress", 0);
        result.put("timestamp", LocalDateTime.now().toString());
        result.put("metadata", Map.of("stage", "STARTED"));

        kafkaTemplate.send(JOB_RESULTS_TOPIC, jobId, result);
        log.info("📤 Notificado INICIO del batch job: {}", jobId);
    }

    /**
     * Notifica progreso del batch job
     */
    public void notifyProgress(String jobId, String status, String message, int progress) {
        Map<String, Object> result = new HashMap<>();
        result.put("jobId", jobId);
        result.put("status", status);
        result.put("message", message);
        result.put("progress", progress);
        result.put("timestamp", LocalDateTime.now().toString());
        result.put("metadata", Map.of("stage", "PROCESSING"));

        kafkaTemplate.send(JOB_RESULTS_TOPIC, jobId, result);
        log.debug("📤 Notificado PROGRESO del batch job {}: {}%", jobId, progress);
    }

    /**
     * Notifica finalización del batch job
     */
    public void notifyCompletion(String jobId, String status, String message,
                                 Map<String, Object> report) {
        Map<String, Object> result = new HashMap<>();
        result.put("jobId", jobId);
        result.put("status", status);
        result.put("message", message);
        result.put("progress", 100);
        result.put("timestamp", LocalDateTime.now().toString());
        result.put("metadata", report != null ? report : Map.of("stage", "COMPLETED"));

        kafkaTemplate.send(JOB_RESULTS_TOPIC, jobId, result);
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

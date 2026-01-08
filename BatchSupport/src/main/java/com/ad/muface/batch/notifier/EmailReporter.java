package com.ad.muface.batch.notifier;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;


@Slf4j
@Service
@RequiredArgsConstructor
public class EmailReporter {

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

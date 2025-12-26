package com.company.batchscheduler.job;

import com.company.batchscheduler.repository.DailySummaryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.annotations.Job;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class CustomerSummaryJob {

    private final DailySummaryRepository dailySummaryRepository;
    private final EmailService emailService;

    // Método modificado para aceptar Strings
    @Job(name = "Generar resumen diario de clientes", retries = 2)
    public void generateDailySummary(String jobId, String processDateStr,
                                     String sendEmailStr, String emailRecipient) {

        try {
            log.info("🚀 Iniciando job {} con fecha: {}", jobId, processDateStr);
            log.info("¿sendEmailStr? " + sendEmailStr);

            // Convertir String a LocalDate
            LocalDate processDate = LocalDate.parse(processDateStr);
            boolean sendEmail = Boolean.parseBoolean(sendEmailStr);

            log.info("Procesando resumen para fecha: {}", processDate);
            if (sendEmail && emailRecipient != null) {
                log.info("📧 Enviando email a: {}", emailRecipient);
                sendSummaryEmail(processDate, jobId, emailRecipient);
                log.info("El job " + jobId + " se ejecutó exitosamente para la fecha " + processDate);
            }

            log.info("✅ Job {} completado exitosamente", jobId);

        } catch (Exception e) {
            log.error("❌ Error en job {}: {}", jobId, e.getMessage(), e);
            throw e; // JobRunr manejará el reintento
        }
    }

    // Método para ejecución inmediata (también con Strings)
    @Job(name = "Ejecución inmediata de resumen")
    public void executeImmediately(String processDateStr, boolean sendEmail, String emailRecipient) {
        String jobId = UUID.randomUUID().toString();
        log.info("Ejecutando job inmediato {} para fecha: {}", jobId, processDateStr);

        // Convertir y procesar
        LocalDate processDate = LocalDate.parse(processDateStr);
        log.info("Procesando resumen para fecha: {}", processDate);
        if (sendEmail && emailRecipient != null) {
            sendSummaryEmail(processDate, jobId, emailRecipient);
            log.info("El job " + jobId + " de ejecución inmediata finalizó de forma exitosa en la fecha " + processDate);
        }
    }

    private void sendSummaryEmail(LocalDate date, String jobId, String recipient) {
        try {
            long count = dailySummaryRepository.countBySummaryDate(date);

            String subject = String.format("📊 Resumen diario procesado - %s", date);
            String body = String.format("""
                <html>
                <body>
                    <h2>Resumen de Procesamiento Batch</h2>
                    <p><strong>Fecha:</strong> %s</p>
                    <p><strong>Job ID:</strong> %s</p>
                    <p><strong>Resúmenes generados:</strong> %d</p>
                    <p><strong>Hora de procesamiento:</strong> %s</p>
                    <br/>
                    <p>Este es un email automático del sistema de Batch Processing.</p>
                </body>
                </html>
                """, date, jobId, count, LocalDateTime.now());

            emailService.sendEmail(recipient, subject, body);
            log.info("📧 Email enviado a: {}", recipient);

        } catch (Exception e) {
            log.warn("⚠️ No se pudo enviar email: {}", e.getMessage());
        }
    }

    public static Date parseDateStr(String dateString) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
            return formatter.parse(dateString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }


}

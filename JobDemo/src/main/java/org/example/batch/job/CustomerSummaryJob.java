package org.example.batch.job;

import common.batch.dto.JobRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.batch.repository.DailySummaryRepository;
import org.jobrunr.jobs.annotations.Job;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Component
@Slf4j
@RequiredArgsConstructor
public class CustomerSummaryJob {
    private final DailySummaryRepository dailySummaryRepository;
    private final EmailService emailService;

    @Job(name = "Generar resumen diario de clientes", retries = 2)
    public String generateDailySummary(JobRequest jobRequest) {

        String jobId = jobRequest.getJobId();
        try {
            String processDateStr = (String) jobRequest.getParameter("date");
            String emailRecipient = (String) jobRequest.getParameter("emailRecipient");
            log.info("🚀 Iniciando job {} con fecha: {} y tipo: {}", jobId, processDateStr, jobRequest.getJobType());

            // Convertir String a LocalDate
            LocalDate processDate = LocalDate.parse(processDateStr);

            Thread.sleep(20000); // 20 segundos

            log.info("Procesando resumen para fecha: {}", processDate);
            if (emailRecipient != null) {
                log.info("📧 Enviando email a: {}", emailRecipient);
                sendSummaryEmail(processDate, jobId, emailRecipient);
                log.info("El job " + jobId + " se ejecutó exitosamente para la fecha " + processDate);
            }

            log.info("✅ Job {} completado exitosamente", jobId);

            return "Proceso ha enviado el correo con toda la info solicitada en fecha " + processDateStr;

        } catch (Exception e) {
            log.error("❌ Error en job {}: {}", jobId, e.getMessage(), e);
            throw new IllegalArgumentException("Unknown job type: " + jobRequest.getJobType());
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


}

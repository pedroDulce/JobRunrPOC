package org.example.batch.job;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.batch.repository.DailySummaryRepository;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class CustomerSummaryJob {

    private final DailySummaryRepository dailySummaryRepository;
    private final EmailService emailService;

    public JobResult executeJob(JobRequest jobRequest, Map<String, String> headers) {

        long mills = Calendar.getInstance().getTimeInMillis();
        JobResult resultado = new JobResult();
        String jobId = jobRequest.getJobId();
        resultado.setJobId(jobId);
        try {
            String processDateStr = (String) jobRequest.getParameters().get("date");
            String emailRecipient = (String) jobRequest.getParameters().get("emailRecipient");
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
            long millsTerminado = Calendar.getInstance().getTimeInMillis();

            log.info("✅ Job {} completado exitosamente", jobId);


            resultado.setMessage("Proceso ha enviado el correo con toda la info solicitada en fecha " + processDateStr);
            resultado.setStatus(JobStatus.SUCCESS);
            resultado.setDurationMs(millsTerminado - mills);
            resultado.setCompletedAt(LocalDateTime.now());

            return resultado;

        } catch (Exception e) {
            long millsTerminado = Calendar.getInstance().getTimeInMillis();
            log.error("❌ Error en job {}: {}", jobId, e.getMessage(), e);
            resultado.setStatus(JobStatus.FAILED);
            resultado.setDurationMs(millsTerminado - mills);
            resultado.setCompletedAt(LocalDateTime.now());
        }
        return resultado;
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

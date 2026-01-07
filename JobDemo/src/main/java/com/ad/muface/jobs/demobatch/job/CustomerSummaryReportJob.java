package com.ad.muface.jobs.demobatch.job;

import com.ad.muface.jobs.demobatch.repository.DailySummaryRepository;
import com.ad.muface.jobs.infra.HeartbeatService;
import com.ad.muface.jobs.infra.JobExecutor;
import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;

@Component
@Slf4j
public class CustomerSummaryReportJob extends JobExecutor {

    private final DailySummaryRepository dailySummaryRepository;

    public CustomerSummaryReportJob(HeartbeatService heartbeatService,
                                       DailySummaryRepository dailySummaryRepository) {
        super(heartbeatService);
        this.dailySummaryRepository = dailySummaryRepository;
    }

    public JobResult executeJobLogic(JobRequest jobRequest) {

        long mills = Calendar.getInstance().getTimeInMillis();
        String jobId = jobRequest.getJobId();
        try {
            LocalDateTime processDateTime = jobRequest.getScheduledAt();
            String emailRecipient = jobRequest.getParameters().get("emailRecipient");
            log.info("🚀 Iniciando job {} con fecha: {} y tipo: {}", jobId, processDateTime, jobRequest.getJobType());

            // Convertir String a LocalDate
            LocalDate processDate = processDateTime.toLocalDate();
            log.info("Procesando resumen1 para fecha-parameter-informe: {}", processDate);
            Thread.sleep(20000); // 20 segundos

            log.info("Procesando resumen2 para fecha-parameter-informe: {}", processDate);
            Thread.sleep(22000); // 20 segundos

            log.info("...procesado resumen para fecha-parameter-informe: {}", processDate);
            if (emailRecipient != null) {
                log.info("📧 Enviando email a: {}", emailRecipient);
                sendSummaryEmail(processDate, jobId, emailRecipient);
                log.info("El job " + jobId + " se ejecutó exitosamente para la fecha " + processDate);
            }
            long millsTerminado = Calendar.getInstance().getTimeInMillis();

            log.info("✅ Job {} completado exitosamente", jobId);

            JobResult resultado = JobResult.builder()
                    .jobId(jobRequest.getJobId())
                    .jobName(jobRequest.getJobName())
                    .status(JobStatusEnum.COMPLETED)
                    .message("Proceso ha enviado el correo con toda la info solicitada en fecha " + processDate)
                    .startedAt(LocalDateTime.now())
                    .completedAt(LocalDateTime.now())
                    .errorDetails(null)
                    .durationMs(millsTerminado - mills)
                    .correlationId(jobRequest.getCorrelationId())
                    .jobrunrJobId(jobRequest.getJobId())  // IMPORTANTE: ID de JobRunr
                    .build();

            return resultado;

        } catch (Exception e) {
            log.error("❌ Error en job {}: {}", jobId, e.getMessage(), e);
            throw new RuntimeException("❌ Error en job {}: {}", e);
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

            this.sendEmail(recipient, subject, body);
            log.info("📧 Email enviado a: {}", recipient);

        } catch (Exception e) {
            log.warn("⚠️ No se pudo enviar email: {}", e.getMessage());
        }
    }

    public void sendEmail(String recipient, String subject, String body) {
        log.info("sending mail... to " + subject + " with body content: " + body);
    }


}

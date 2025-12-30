package org.example.batch.notifier;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.batch.service.BatchJobExecutorService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchStatusNotifier {

    private final BatchJobExecutorService batchJobExecutorService;
    private final KafkaPublisher kafkaPublisher;

    @Transactional
    public void consumeBatchRequest(
            ConsumerRecord<String, JobRequest> record,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = "business-domain", required = true) String businessDomain,
            @Header(value = "target-batch", required = true) String targetBatch,
            @Header(value = "priority", defaultValue = "MEDIUM") String priority,
            @Header(value = "correlation-id", required = true) String correlationId,
            @Header(value = "jobrunr-job-id", required = true) String jobrunrJobId,
            @Payload Map<String, Object> payload,
            Acknowledgment acknowledgment) {

        JobResult result;
        try {
            JobRequest jobRequest = record.value();

            log.info("""
                    📥 JobExecutor: Received Batch Request:
                    Job ID: {}
                    JobRunr Job ID: {}
                    Business Domain: {}
                    Target Batch: {}
                    Priority: {}
                    Correlation ID: {}
                    """,
                    jobRequest.getJobId(),
                    jobrunrJobId,
                    businessDomain,
                    targetBatch,
                    priority,
                    correlationId
            );

            // 1. Publicar estado IN_PROGRESS
            kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.IN_PROGRESS, null,
                    correlationId, jobrunrJobId, "JobExecutor: remote Batch execution started");

            // 2. Ejecutar el batch
            batchJobExecutorService.executeSpringBatchJob(jobrunrJobId, jobRequest.getJobName(),
                    jobRequest.getParameters());

            // 4. Confirmar offset
            acknowledgment.acknowledge();

            log.info("✅ JobExecutor: Batch {} executed successfully", jobRequest.getJobId());

        } catch (Exception e) {
            log.error("❌ JobExecutor: Error processing Batch request: {}", e.getMessage(), e);

            // Publicar estado FAILED si hay jobRequest
            if (record != null && record.value() != null) {
                JobRequest jobRequest = record.value();
                kafkaPublisher.publishJobStatus(jobRequest, JobStatusEnum.FAILED, e,
                        correlationId, jobrunrJobId, "Batch execution failed: " + e.getMessage());
            }

            // No confirmar para que se reintente
        }
    }

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
        kafkaPublisher.publishToResultsTopic(statusResult);

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
        kafkaPublisher.publishToResultsTopic(statusResult);

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
        kafkaPublisher.publishToResultsTopic(statusResult);

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

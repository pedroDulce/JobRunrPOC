package com.ad.muface.batch.scheduler.remotesender;

import com.ad.muface.batch.dto.JobRequest;
import com.ad.muface.batch.dto.JobResult;
import com.ad.muface.batch.scheduler.service.JobManagementOperations;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class RemoteBatchRestInvoker {

    private final RestTemplate restJobResultTemplate;

    private final RestTemplate restTemplate;

    private final JobManagementOperations jobManagementOperations;

    private final StatusbeatService statusbeatService;

    @Value("${dispatcher.batch-runner-endpoint}")
    private String batchRunnerEndpoint;
    @Value("${dispatcher.batch-status-batch-endpoint}")
    private String batchStatusEndpoint;

    @Value("${dispatcher.batch-heartbeats-frequency}")
    private int batchHeartbeatsFrequency;

    /**
     * Envía un JobRequest al batch runner para ejecución
     *
     * @param jobRequest Objeto JobRequest con todos los parámetros
     * @param jobContext Objeto JobContext de JobRunr
     * @return Respuesta del batch runner
     */
    @Job(name= "Batch Job remoto")
    public JobResult invocarJobRemoto(JobRequest jobRequest, JobContext jobContext) {
        try {
            jobRequest.setJobId(jobContext.getJobId().toString());
            jobRequest.setCorrelationId(jobManagementOperations.getRecurringIdJobByName(jobRequest.getJobName()));

            log.info("Enviando JobRequest al batch runner: {}", jobRequest.getJobId());

            // Validar que el jobRequest tenga los datos mínimos
            validateJobRequest(jobRequest);

            // Construir la URL con el jobName como path variable
            String url = UriComponentsBuilder.fromHttpUrl(jobRequest.getUrlMicroDestino())
                    .path(batchRunnerEndpoint)
                    .buildAndExpand(jobRequest.getJobName())
                    .toUriString();

            log.info("URL destino: {}", url);

            // Configurar headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

            // Agregar headers adicionales si es necesario
            addCustomHeaders(headers, jobRequest);

            // Crear la entidad HTTP con el JobRequest en el body
            HttpEntity<JobRequest> requestEntity = new HttpEntity<>(jobRequest, headers);

            // Realizar la petición POST para el inicio (/run) del batch remoto
            ResponseEntity<JobResult> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    requestEntity,
                    JobResult.class
            );

            org.jobrunr.jobs.Job job = jobManagementOperations.getJobById(jobContext.getJobId());
            if (job == null) {
                log.warn("JOB SCHEDULER::: JobRunr job {} not found in storage or was deleted", jobContext.getJobId());
            } else {
                JobResult resInicioEjecucionRemota = response.getBody();
                if (!resInicioEjecucionRemota.isFailed()) {
                    Long executionId = resInicioEjecucionRemota.getExecutionId();
                    log.info("📨 JOB SCHEDULER:::Received remoteBatchExecutionId {} for JobRunr Job name {} and Job ID: {}",
                            executionId, jobContext.getJobId(), jobRequest.getJobName());
                    JobResult jobResult = new JobResult();
                    jobResult.setJobName(jobRequest.getJobName());
                    jobResult.setStartedAt(LocalDateTime.now());
                    jobManagementOperations.updateJobRunrStatus(job, jobResult);

                    iniciarMonitoreo(executionId, jobRequest);
                }
            }

            // Procesar la respuesta
            return processResponseRemoteInvocation(response, jobRequest);

        } catch (Exception e) {
            log.error("Error enviando JobRequest al batch runner: {}", jobRequest.getJobId(), e);
            throw new RuntimeException("Error al enviar job al batch runner", e);
        }
    }

    /**
     * Método que inicia el monitoreo periódico
     */
    public void iniciarMonitoreo(Long executionId, JobRequest jobRequest) {
        String taskId = "monitor-" + executionId + "-" + jobRequest.getJobId();

        // Crear un Runnable que ejecute tu método
        Runnable statusCheckTask = () -> {
            try {
                // Tu método se ejecuta aquí, el retorno se ignora porque ya lo procesas internamente
                this.getJobExecutionStatus(executionId, jobRequest);
            } catch (Exception e) {
                // Manejo adicional si es necesario
                log.error("Error en verificación de estado para ejecución: {}", executionId, e);
            }
        };

        // Iniciar el heartbeat con el Runnable
        statusbeatService.startHeartbeat(
                taskId,           // Identificador único
                statusCheckTask,  // La tarea a ejecutar
                0,                // Delay inicial (0 = inmediato)
                batchHeartbeatsFrequency,                // Cada x segundos
                TimeUnit.SECONDS
        );

        log.info("Monitoreo iniciado para ejecución: {}", executionId);
    }


    /**
     * Método para validar el JobRequest
     */
    private void validateJobRequest(JobRequest jobRequest) {
        if (jobRequest == null) {
            throw new IllegalArgumentException("JobRequest no puede ser nulo");
        }

        if (jobRequest.getJobId() == null || jobRequest.getJobId().trim().isEmpty()) {
            throw new IllegalArgumentException("JobId es requerido");
        }

        if (jobRequest.getJobName() == null || jobRequest.getJobName().trim().isEmpty()) {
            throw new IllegalArgumentException("JobName es requerido");
        }

        log.debug("JobRequest validado: jobId={}, jobName={}, parameters={}",
                jobRequest.getJobId(),
                jobRequest.getJobName(),
                jobRequest.getParameters() != null ? jobRequest.getParameters().size() : 0);
    }

    /**
     * Agregar headers personalizados basados en el JobRequest
     */
    private void addCustomHeaders(HttpHeaders headers, JobRequest jobRequest) {
        // Header para correlación
        if (jobRequest.getCorrelationId() != null) {
            headers.set("X-Correlation-ID", jobRequest.getCorrelationId());
        }

        // Header para prioridad
        if (jobRequest.getPriority() != null) {
            headers.set("X-Job-Priority", jobRequest.getPriority());
        }

        // Header para TTL si existe
        if (jobRequest.getTtl() != null) {
            headers.set("X-Job-TTL", jobRequest.getTtl().toString());
        }

        // Header para el creador
        if (jobRequest.getCreatedBy() != null) {
            headers.set("X-Job-Created-By", jobRequest.getCreatedBy());
        }

        // Headers para métricas
        headers.set("X-Request-Timestamp", String.valueOf(System.currentTimeMillis()));
        headers.set("User-Agent", "BatchJobDispatcher/1.0");
    }

    /**
     * Procesar la respuesta del batch runner
     */
    private JobResult processResponseRemoteInvocation(ResponseEntity<JobResult> response, JobRequest jobRequest) {
        HttpStatusCode statusCode = response.getStatusCode();

        log.info("Respuesta recibida del batch runner para job {}: Status={}, Body={}",
                jobRequest.getJobId(), statusCode, response.getBody());

        if (statusCode.is2xxSuccessful()) {
            log.info("Job {} enviado exitosamente al batch runner", jobRequest.getJobId());
            return response.getBody();
        } else if (statusCode == HttpStatus.NOT_FOUND) {
            throw new RuntimeException("Endpoint del batch runner no encontrado");
        } else if (statusCode == HttpStatus.BAD_REQUEST) {
            throw new RuntimeException("Solicitud inválida: " + response.getBody());
        } else if (statusCode == HttpStatus.INTERNAL_SERVER_ERROR) {
            throw new RuntimeException("Error interno del batch runner: " + response.getBody());
        } else {
            throw new RuntimeException("Respuesta inesperada: " + statusCode + " - " + response.getBody());
        }
    }


    /************* METODOS PARA CONTROLAR EL ESTADO DEL REMOTO ***********/
    /**
     * Consulta el estado de una ejecución de job por su ID
     *
     * @param executionId ID de la ejecución del job
     * @return Mapa con la información del estado
     */
    public String getJobExecutionStatus(Long executionId, JobRequest jobRequest) {
        try {
            log.info("Consultando estado de ejecución: {}", executionId);

            // Construir la URL
            String url = UriComponentsBuilder.fromHttpUrl(jobRequest.getUrlMicroDestino())
                    .path(batchStatusEndpoint)
                    .buildAndExpand(executionId)
                    .toUriString();

            log.debug("URL de consulta: {}", url);

            // Configurar headers
            HttpHeaders headers = new HttpHeaders();
            headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            headers.set("X-Client", "BatchStatusClient");

            HttpEntity<String> entity = new HttpEntity<>(headers);

            // Realizar la petición GET
            ResponseEntity<JobResult> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    entity,
                    JobResult.class
            );

            // Procesar la respuesta
            JobResult jobResult = processResponseRemoteInvocation(response, jobRequest);

            // Si el job ha terminado, detener el heartbeat
            if (jobResult.getCompletedAt() != null) {
                String taskId = "monitor-" + executionId + "-" + jobRequest.getJobId();
                statusbeatService.stopHeartbeat(taskId);
                log.info("Heartbeat detenido porque job {} ha completado", jobRequest.getJobId());
            }

            org.jobrunr.jobs.Job job = this.jobManagementOperations
                    .getJobById(UUID.fromString(jobResult.getJobId()));

            this.jobManagementOperations.updateJobRunrStatus(job, jobResult);

            return jobRequest.getJobId();

        } catch (Exception e) {
            log.error("Error consultando estado de ejecución {}: {}", executionId, e.getMessage(), e);
            throw new RuntimeException("Error consultando estado de ejecución: " + executionId, e);
        }
    }



}

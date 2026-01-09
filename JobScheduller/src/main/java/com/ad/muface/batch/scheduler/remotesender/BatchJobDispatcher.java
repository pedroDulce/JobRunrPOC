package com.ad.muface.batch.scheduler.remotesender;

import com.ad.muface.batch.dto.JobRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class BatchJobDispatcher {

    private final RestTemplate restTemplate;

    @Value("${dispatcher.batch-runner-endpoint}")
    private String batchRunnerEndpoint;

    /**
     * Envía un JobRequest al batch runner para ejecución
     *
     * @param jobRequest Objeto JobRequest con todos los parámetros
     * @param jobContext Objeto JobContext de JobRunr
     * @return Respuesta del batch runner
     */
    @Job(name= "Sync Job")
    public String invocarJobRemoto(JobRequest jobRequest, JobContext jobContext) {
        try {
            jobRequest.setJobId(jobContext.getJobId().toString());

            log.info("Enviando JobRequest al batch runner: {}", jobRequest.getJobId());

            // Validar que el jobRequest tenga los datos mínimos
            validateJobRequest(jobRequest);

            // Construir la URL con el jobName como path variable
            String url = UriComponentsBuilder.fromHttpUrl(jobRequest.getUrlDestino())
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

            // Realizar la petición POST
            ResponseEntity<String> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    requestEntity,
                    String.class
            );

            // Procesar la respuesta
            return processResponse(response, jobRequest);

        } catch (Exception e) {
            log.error("Error enviando JobRequest al batch runner: {}", jobRequest.getJobId(), e);
            throw new RuntimeException("Error al enviar job al batch runner", e);
        }
    }

    /**
     * Método asíncrono para enviar el job (no bloqueante)
     */
    public void dispatchJobAsync(JobRequest jobRequest, JobContext jobContext) {
        CompletableFuture.runAsync(() -> {
            try {
                String response = invocarJobRemoto(jobRequest, jobContext);
                log.info("Job enviado asíncronamente: {} - Respuesta: {}",
                        jobRequest.getJobId(), response);
            } catch (Exception e) {
                log.error("Error en envío asíncrono del job: {}", jobRequest.getJobId(), e);
            }
        });
    }

    /**
     * Método con retry automático
     */
    public String dispatchJobWithRetry(JobRequest jobRequest, JobContext jobContext, int maxRetries) {
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount <= maxRetries) {
            try {
                log.info("Intento {} de {} para job: {}",
                        retryCount + 1, maxRetries + 1, jobRequest.getJobId());

                return invocarJobRemoto(jobRequest, jobContext);

            } catch (Exception e) {
                lastException = e;
                retryCount++;

                if (retryCount <= maxRetries) {
                    log.warn("Intento {} fallido para job: {}. Reintentando...",
                            retryCount, jobRequest.getJobId());

                    // Esperar exponencialmente antes de reintentar
                    try {
                        long waitTime = (long) Math.pow(2, retryCount) * 1000; // Backoff exponencial
                        Thread.sleep(Math.min(waitTime, 10000)); // Máximo 10 segundos
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupción durante reintento", ie);
                    }
                }
            }
        }

        throw new RuntimeException("Falló después de " + maxRetries + " reintentos", lastException);
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
    private String processResponse(ResponseEntity<String> response, JobRequest jobRequest) {
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


}

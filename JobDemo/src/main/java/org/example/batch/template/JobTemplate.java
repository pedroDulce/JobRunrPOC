package org.example.batch.template;

import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.annotations.Job;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public abstract class JobTemplate {

    /**
     * PLANTILLA BASE de la que todos los teams extenderán para crear sus jobs.
     * TODO : añadirla en el backend-infra
     */
    @Job(name = "%s", retries = 3)  // Nombre dinámico
    public void executeTemplateJob(
            String jobName,
            String jobType,
            String parametersJson,
            String callbackUrl) {

        log.info("🚀 Ejecutando job '{}' de tipo: {}", jobName, jobType);

        try {
            // 1. Parsear parámetros
            Map<String, Object> params = parseParameters(parametersJson);

            // 2. Ejecutar según tipo
            switch (jobType) {
                case "SQL_QUERY":
                    executeSqlJob(params);
                    break;
                case "FILE_PROCESSING":
                    executeFileJob(params);
                    break;
                case "REST_CALL":
                    executeRestJob(params);
                    break;
                default:
                    executeCustomJob(jobType, params);
            }

            // 3. Notificar éxito
            if (callbackUrl != null) {
                notifySuccess(callbackUrl, jobName);
            }

            log.info("✅ Job '{}' completado exitosamente", jobName);

        } catch (Exception e) {
            log.error("❌ Job '{}' falló: {}", jobName, e.getMessage());
            // Notificar fallo
            if (callbackUrl != null) {
                notifyFailure(callbackUrl, jobName, e.getMessage());
            }
            throw e; // JobRunr maneja el reintento
        }
    }

    protected abstract Map<String, Object> parseParameters(String parametersJson);
    protected abstract void executeRestJob(Map<String, Object> params);
    protected abstract void executeSqlJob(Map<String, Object> params);
    protected abstract void executeFileJob(Map<String, Object> params);
    protected abstract void executeCustomJob(String jobType, Map<String, Object> params);
    protected abstract void notifyFailure(String callbackUrl, String jobName, String message);
    protected abstract void notifySuccess(String callbackUrl, String jobName);


    /** EJEMPLOS **/
    /*protected void executeSqlJob(Map<String, Object> params) {
        String query = (String) params.get("query");
        String dataSource = (String) params.get("dataSource");

        log.info("Ejecutando query en {}: {}", dataSource, query);
        // Tu lógica JDBC/JPA aquí
    }

    protected void executeRestJob(Map<String, Object> params) {
        String url = (String) params.get("url");
        String method = params.get("method") == null ? "POST" : (String) params.get("method");
        String body = (String) params.get("body");

        log.info("Llamando REST {} a: {}", method, url);
        // Usar RestTemplate para llamar al microservicio
    }*/

}

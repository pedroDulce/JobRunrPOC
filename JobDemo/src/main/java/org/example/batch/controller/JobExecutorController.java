package org.example.batch.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.batch.JobRequest;
import org.example.batch.JobResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@Slf4j
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobExecutorController {

    @PostMapping("/execute")
    public ResponseEntity<JobResult> executeJob(@RequestBody JobRequest request) {
        log.info("Recibido job: {}", request.getJobType());

        try {
            // Ejecutar según tipo
            Object result = switch (request.getJobType()) {
                case "customer-summary" -> processCustomerSummary(request);
                case "report-generation" -> generateReport(request);
                case "data-migration" -> migrateData(request);
                default -> throw new IllegalArgumentException("Tipo desconocido");
            };
            return ResponseEntity.ok(JobResult.success(result));
        } catch (Exception e) {
            log.error("Job falló: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(JobResult.failure(e.getMessage()));
        }
    }

    private Object migrateData(JobRequest request) {
        return "Prueba de migrateData processing";
    }

    private Object generateReport(JobRequest request) {
        return "Prueba de generateReport processing";
    }

    private Object processCustomerSummary(JobRequest request) {
        // Lógica específica del team
        // Leer de BD, procesar, escribir resultados
        return Map.of("processed", 1500, "success", true);
    }

}


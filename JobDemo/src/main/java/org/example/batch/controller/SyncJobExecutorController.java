package org.example.batch.controller;

import common.batch.dto.JobRequest;
import common.batch.dto.JobResult;
import common.batch.dto.JobStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.Map;

@RestController
@Slf4j
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class SyncJobExecutorController {

    @PostMapping("/execute-sync")
    public ResponseEntity<JobResult> executeJob(@RequestBody JobRequest request) {
        log.info("[JOB—DEMO]::JobExecutorController:: Recibido job: {}", request.getJobType());

        try {
            // Ejecutar
            Object result = processCustomerSummary(request);
            log.info("[JOB—DEMO]::JobExecutorController:: Job invocado vìa REST (modo sync) finalizó con éxito");
            return ResponseEntity.ok(new JobResult(request.getJobId(), JobStatusEnum.COMPLETED, result.toString(),
                    LocalDateTime.now()));
        } catch (Exception e) {
            log.error("Job falló: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(new JobResult(request.getJobId(), JobStatusEnum.FAILED,
                    e.getMessage(),
                    LocalDateTime.now()));
        }
    }

    private Object processCustomerSummary(JobRequest request) {
        // Lógica específica del team
        // Leer de BD, procesar, escribir resultados
        return Map.of("processed", 1500, "success", true);
    }

}


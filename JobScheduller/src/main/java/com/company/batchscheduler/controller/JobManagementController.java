package com.company.batchscheduler.controller;

import com.company.batchscheduler.model.JobStatus;
import com.company.batchscheduler.repository.JobStatusRepository;
import com.company.batchscheduler.service.JobService;
import com.company.batchscheduler.service.JobTrackingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobManagementController {

    private final JobService jobService;
    private final JobStatusRepository statusRepository;
    private final JobTrackingService jobTrackingService;

    @GetMapping("/status/{jobId}")
    public ResponseEntity<JobStatus> getStatus(@PathVariable String jobId) {
        return statusRepository.findByJobId(jobId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * DELETE /api/jobs/{jobId}
     * Eliminar un job específico
     */
    @DeleteMapping("/{jobId}")
    public ResponseEntity<Map<String, Object>> deleteJob(@PathVariable String jobId) {
        boolean deleted = jobService.deleteJob(jobId);

        if (deleted) {
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Job deleted successfully",
                    "jobId", jobId
            ));
        } else {
            return ResponseEntity.status(404).body(Map.of(
                    "success", false,
                    "message", "Job not found or could not be deleted",
                    "jobId", jobId
            ));
        }
    }

    @GetMapping("/{jobrunrJobId}/status")
    public ResponseEntity<Map<String, Object>> getJobStatus(@PathVariable String jobrunrJobId) {
        try {
            Map<String, Object> status = jobTrackingService.getCombinedStatus(jobrunrJobId);

            if (status.containsKey("error") && "Tracking not found".equals(status.get("error"))) {
                return ResponseEntity.notFound().build();
            }

            return ResponseEntity.ok(status);

        } catch (Exception e) {
            log.error("Error getting status for {}: {}", jobrunrJobId, e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/search")
    public ResponseEntity<Map<String, Object>> searchJobs(
            @RequestParam(required = false) String executorJobId,
            @RequestParam(required = false) String correlationId,
            @RequestParam(required = false) String status) {

        // Implementar búsqueda según tus necesidades
        return ResponseEntity.ok(Map.of(
                "message", "Search endpoint",
                "executorJobId", executorJobId,
                "correlationId", correlationId,
                "status", status
        ));
    }

}

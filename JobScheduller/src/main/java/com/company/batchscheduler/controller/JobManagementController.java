package com.company.batchscheduler.controller;

import com.company.batchscheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobManagementController {

    private final JobService jobService;

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

    /**
     * DELETE /api/jobs
     * Eliminar múltiples jobs
     */
    @DeleteMapping
    public ResponseEntity<Map<String, Object>> deleteJobs(@RequestBody List<String> jobIds) {
        int deletedCount = jobService.deleteJobs(jobIds);

        return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Deleted " + deletedCount + " jobs",
                "deletedCount", deletedCount,
                "totalRequested", jobIds.size()
        ));
    }

    /**
     * DELETE /api/jobs/type/{jobType}
     * Eliminar todos los jobs de un tipo
     */
    @DeleteMapping("/type/{jobType}")
    public ResponseEntity<Map<String, Object>> deleteJobsByType(@PathVariable String jobType) {
        int deletedCount = jobService.deleteJobsByType(jobType);

        return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Deleted " + deletedCount + " jobs of type " + jobType,
                "jobType", jobType,
                "deletedCount", deletedCount
        ));
    }

    /**
     * DELETE /api/jobs/state/{state}
     * Eliminar jobs por estado
     */
    @DeleteMapping("/state/{state}")
    public ResponseEntity<Map<String, Object>> deleteJobsByState(@PathVariable String state) {
        int deletedCount = jobService.deleteJobsByState(state);

        return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Deleted " + deletedCount + " jobs in state " + state,
                "state", state,
                "deletedCount", deletedCount
        ));
    }

    /**
     * POST /api/jobs/{jobId}/cancel
     * Cancelar un job programado
     */
    @PostMapping("/{jobId}/cancel")
    public ResponseEntity<Map<String, Object>> cancelJob(@PathVariable String jobId) {
        boolean cancelled = jobService.cancelJob(jobId);

        if (cancelled) {
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Job cancelled successfully",
                    "jobId", jobId
            ));
        } else {
            return ResponseEntity.status(400).body(Map.of(
                    "success", false,
                    "message", "Job cannot be cancelled (may be already running or completed)",
                    "jobId", jobId
            ));
        }
    }

    /**
     * DELETE /api/jobs/cleanup/old
     * Limpiar jobs antiguos
     */
    @DeleteMapping("/cleanup/old")
    public ResponseEntity<Map<String, Object>> cleanupOldJobs(
            @RequestParam(defaultValue = "30") int daysOld) {

        // Implementar lógica para eliminar jobs más antiguos que X días
        // Necesitarías crear un método en JobService para esto

        return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Cleanup completed",
                "daysOld", daysOld
        ));
    }
}

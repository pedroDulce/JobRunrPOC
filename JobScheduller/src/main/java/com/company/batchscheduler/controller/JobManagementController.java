package com.company.batchscheduler.controller;

import com.company.batchscheduler.model.JobStatus;
import com.company.batchscheduler.repository.JobStatusRepository;
import com.company.batchscheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobManagementController {

    private final JobService jobService;

    private final JobStatusRepository statusRepository;

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


}

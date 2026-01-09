package com.ad.muface.batch.controller;

import com.ad.muface.batch.dto.JobRequest;
import com.ad.muface.batch.utilities.JobMetadataUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@Slf4j
@RequestMapping("/batch-runner")
public class BatchController {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;
    private final JobExplorer jobExplorer;
    private final ObjectMapper objectMapper; // P

    public BatchController(JobLauncher jobLauncher, JobRegistry jobRegistry, JobExplorer jobExplorer, ObjectMapper objectMapper) {
        this.jobLauncher = jobLauncher;
        this.jobRegistry = jobRegistry;
        this.jobExplorer = jobExplorer;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/run/{jobName}")
    public String run(@PathVariable String jobName, @RequestBody String requestBody) throws Exception {

        Job job = jobRegistry.getJob(jobName);

        // Convertir el cuerpo de la petición a JobRequest
        JobRequest jobRequest = objectMapper.readValue(requestBody, JobRequest.class);

        log.info("""
                        📥 JobExecutor: Recibida Job Request via API REST:
                        Remote Job ID: {}
                        Business Domain: {}
                        Target Job Batch name: {}
                        Priority: {}
                        Parent Recurring Job Remote ID: {}
                        """,
                jobRequest.getJobId(),
                jobRequest.getBusinessDomain(),
                jobRequest.getJobName(),
                jobRequest.getPriority(),
                jobRequest.getCorrelationId()
        );
        // 1: Construir parámetros del job
        JobParameters jobParameters = JobMetadataUtils.createJobRequestParameters(jobRequest);

        JobExecution exec = jobLauncher.run(job, jobParameters);

        return "executionId=" + exec.getId();
    }

    @GetMapping("/status/{executionId}")
    public Map<String, Object> status(@PathVariable Long executionId) {
        JobExecution exec = jobExplorer.getJobExecution(executionId);
        Map<String, Object> map = new HashMap<>();
        if (exec != null) {
            map.put("jobName", exec.getJobInstance().getJobName());
            map.put("status", exec.getStatus().toString());
        }
        return map;
    }
}

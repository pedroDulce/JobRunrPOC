package com.ad.muface.batch.controller;

import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/batch-runner")
public class BatchController {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;
    private final JobExplorer jobExplorer;

    public BatchController(JobLauncher jobLauncher, JobRegistry jobRegistry, JobExplorer jobExplorer) {
        this.jobLauncher = jobLauncher;
        this.jobRegistry = jobRegistry;
        this.jobExplorer = jobExplorer;
    }

    @PostMapping("/run/{jobName}")
    public String run(@PathVariable String jobName) throws Exception {
        Job job = jobRegistry.getJob(jobName);
        JobExecution exec = jobLauncher.run(
                job,
                new JobParametersBuilder()
                        .addLong("run.id", System.currentTimeMillis())
                        .toJobParameters()
        );
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

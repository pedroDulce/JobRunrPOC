package com.ad.muface.batch.controller;

import com.ad.muface.batch.dto.JobRequest;
import com.ad.muface.batch.dto.JobResult;
import com.ad.muface.batch.dto.JobStatusEnum;
import com.ad.muface.batch.utilities.JobMetadataUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;

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
    public JobResult run(@PathVariable String jobName, @RequestBody String requestBody) throws Exception {

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
        // Construir parámetros del job
        JobParameters jobParameters = JobMetadataUtils.createJobRequestParameters(jobRequest);

        JobExecution exec = jobLauncher.run(job, jobParameters);

        JobResult jobResult = new JobResult();
        if (exec != null) {
            jobResult.setExecutionId(exec.getId());
            jobResult.setJobId(exec.getJobParameters().getString("externalJobId"));
            jobResult.setJobName(exec.getJobInstance().getJobName());
            jobResult.setMessage("Iniciado el batch-job-remoto con id: " + jobResult.getExecutionId());

            log.info("BatchController::Iniciado el batch-job-remoto con id {} ", jobResult.getExecutionId());
            log.info("BatchController::Iniciado el batch-job del batch registrado en JobScheduler como {} , ID {}: ",
                    jobResult.getJobName(),
                    jobResult.getJobId());

            BatchStatus status = exec.getStatus();
            if (status == BatchStatus.COMPLETED) {
                jobResult.setStatus(JobStatusEnum.COMPLETED);
            } else if (status == BatchStatus.FAILED || status == BatchStatus.STOPPED) {
                jobResult.setStatus(JobStatusEnum.FAILED);
            } else {
                jobResult.setStatus(JobStatusEnum.IN_PROGRESS);
            }
            jobResult.setStartedAt(exec.getStartTime());
        }
        return jobResult;
    }

    @GetMapping("/status/{executionId}")
    public JobResult status(@PathVariable Long executionId) {
        JobExecution exec = jobExplorer.getJobExecution(executionId);
        JobResult jobResult = new JobResult();
        if (exec != null) {
            log.info("BatchController::Solicitado estado del batch-job-remoto con id {} ", jobResult.getExecutionId());
            jobResult.setExecutionId(executionId);
            jobResult.setJobId(exec.getJobParameters().getString("externalJobId"));
            jobResult.setJobName(exec.getJobInstance().getJobName());
            log.info("BatchController::Solicitado estado del batch-job-remoto registrado en JobScheduler como {} , ID {}: ",
                    jobResult.getJobName(),
                    jobResult.getJobId());
            BatchStatus status = exec.getStatus();
            if (status == BatchStatus.COMPLETED) {
                jobResult.setMessage("Finalizado el batch-job-remoto con id: " + jobResult.getExecutionId());
                jobResult.setStatus(JobStatusEnum.COMPLETED);
            } else if (status == BatchStatus.FAILED || status == BatchStatus.STOPPED) {
                jobResult.setMessage("Ha fallado la ejecución del batch-job-remoto con id: " + jobResult.getExecutionId());
                jobResult.setStatus(JobStatusEnum.FAILED);
            } else {
                jobResult.setMessage("Permanece en ejecución el batch-job-remoto con id: " + jobResult.getExecutionId());
                jobResult.setStatus(JobStatusEnum.IN_PROGRESS);
            }
            jobResult.setStartedAt(exec.getStartTime());
            jobResult.setLastHeartBeat(exec.getLastUpdated());
            jobResult.setCompletedAt(exec.getEndTime());
            if (exec.getStatus() == BatchStatus.COMPLETED && exec.getEndTime() != null) {
                long duracionMs = Duration.between(exec.getStartTime(), exec.getEndTime()).toMillis();
                jobResult.setExecutionTimeInMills(duracionMs);
            }
        }
        return jobResult;
    }
}

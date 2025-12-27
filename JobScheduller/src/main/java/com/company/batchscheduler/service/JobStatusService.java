package com.company.batchscheduler.service;

import com.company.batchscheduler.model.JobStatus;
import com.company.batchscheduler.repository.JobStatusRepository;
import common.batch.dto.JobRequest;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
@Transactional
@Slf4j
@RequiredArgsConstructor
public class JobStatusService {

    private final JobStatusRepository repository;

    public Optional<JobStatus> findByJobId(String jobId) {
        return repository.findByJobId(jobId);
    }

    public JobStatus saveOrUpdate(JobStatus job) {
        // Buscar si ya existe
        Optional<JobStatus> existing = repository.findByJobId(job.getJobId());

        if (existing.isPresent()) {
            // Actualizar existente
            JobStatus status = existing.get();
            status.setStatus(job.getStatus());
            status.setMessage(job.getMessage());
            status.setUpdatedAt(LocalDateTime.now());
            return repository.save(status);
        } else {
            // Crear nuevo
            JobStatus status = JobStatus.builder()
                    .jobId(job.getJobId())
                    .jobType(job.getJobType())
                    .status(job.getStatus())
                    .message(job.getMessage())
                    .createdAt(LocalDateTime.now())
                    .build();
            return repository.save(status);
        }
    }
}

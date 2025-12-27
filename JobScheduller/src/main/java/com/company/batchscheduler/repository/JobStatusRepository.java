package com.company.batchscheduler.repository;

import com.company.batchscheduler.model.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface JobStatusRepository extends JpaRepository<JobStatus, String> {
    Optional<JobStatus> findByJobId(String jobId);

    boolean existsByJobId(String jobId);
}
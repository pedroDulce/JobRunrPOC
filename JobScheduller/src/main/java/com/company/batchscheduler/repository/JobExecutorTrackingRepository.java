package com.company.batchscheduler.repository;

import com.company.batchscheduler.model.JobExecutorTracking;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface JobExecutorTrackingRepository extends JpaRepository<JobExecutorTracking, Long> {

    Optional<JobExecutorTracking> findByJobrunrJobId(String jobrunrJobId);

    Optional<JobExecutorTracking> findByExecutorJobId(String executorJobId);

    Optional<JobExecutorTracking> findByCorrelationId(String correlationId);
}

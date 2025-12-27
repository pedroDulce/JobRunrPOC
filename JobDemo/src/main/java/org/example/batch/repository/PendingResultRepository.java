package org.example.batch.repository;

import org.example.batch.model.PendingResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PendingResultRepository extends JpaRepository<PendingResult, Long> {

    Optional<PendingResult> findByJobId(String jobId);

    List<PendingResult> findByStatusAndAttemptsLessThan(String status, int maxAttempts);

    List<PendingResult> findByStatus(String status);

    void deleteByJobId(String jobId);
}

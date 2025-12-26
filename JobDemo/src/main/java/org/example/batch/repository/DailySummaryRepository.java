package org.example.batch.repository;

import com.company.batchscheduler.model.DailySummary;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDate;

public interface DailySummaryRepository extends JpaRepository<DailySummary, Long> {
    @Query("SELECT COUNT(*) FROM DailySummary da WHERE da.summaryDate = :date")
    long countBySummaryDate(LocalDate date);

}

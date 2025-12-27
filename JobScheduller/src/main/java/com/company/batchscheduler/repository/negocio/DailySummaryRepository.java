package com.company.batchscheduler.repository.negocio;

import com.company.batchscheduler.model.negocio.DailySummary;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDate;

public interface DailySummaryRepository extends JpaRepository<DailySummary, Long> {
    @Query("SELECT COUNT(*) FROM DailySummary da WHERE da.summaryDate = :date")
    long countBySummaryDate(LocalDate date);

}

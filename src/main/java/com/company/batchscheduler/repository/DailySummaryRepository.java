package com.company.batchscheduler.repository;

import com.company.batchscheduler.model.DailySummary;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.Date;

public interface DailySummaryRepository extends JpaRepository<DailySummary, Long> {
    @Query("SELECT COUNT(*) FROM DailySummary da WHERE da.summaryDate = :date")
    long countBySummaryDate(Date date);

}

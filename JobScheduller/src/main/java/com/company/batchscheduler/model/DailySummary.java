package com.company.batchscheduler.model;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "daily_summaries")
@Data
public class DailySummary {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "summary_date")
    private LocalDate summaryDate;

    @Column(name = "customer_id", length = 50)
    private String customerId;

    @Column(name = "total_amount", precision = 10, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "transaction_count")
    private Integer transactionCount;

    @Column(length = 3)
    private String currency;

    @Column(name = "processed_at")
    private LocalDateTime processedAt;

    @Column(name = "job_execution_id", length = 100)
    private String jobExecutionId;
}

package com.company.batchscheduler.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Entity
@Table(name = "job_executor_tracking")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobExecutorTracking {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "jobrunr_job_id", nullable = false, unique = true)
    private String jobrunrJobId;

    @Column(name = "executor_job_id")
    private String executorJobId;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "executor_status")
    private String executorStatus; // IN_PROGRESS, COMPLETED, FAILED, CANCELLED

    @Column(name = "jobrunr_state")
    private String jobrunrState; // ENQUEUED, PROCESSING, SUCCEEDED, FAILED

    @Column(name = "message")
    private String message;

    @Column(name = "error_details", columnDefinition = "TEXT")
    private String errorDetails;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata", columnDefinition = "jsonb")
    @Builder.Default
    private Map<String, Object> metadata = new HashMap<>();

    @CreationTimestamp
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    // Métodos de ayuda
    public void updateStatus(String status, String message) {
        this.executorStatus = status;
        this.message = message;
        this.updatedAt = LocalDateTime.now();

        if ("COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status)) {
            this.completedAt = LocalDateTime.now();
        }
    }

    public void addMetadata(String key, Object value) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        this.metadata.put(key, value);
    }
}

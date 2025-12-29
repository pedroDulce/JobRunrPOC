package org.example.batch.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "pending_results", indexes = {
        @Index(name = "idx_job_status", columnList = "status"),
        @Index(name = "idx_created_at", columnList = "created_at"),
        @Index(name = "idx_correlation_id", columnList = "correlation_id")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PendingResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false, unique = true)
    private String jobId;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "result_data", columnDefinition = "TEXT")
    private String resultData;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "attempts")
    @Builder.Default
    private Integer attempts = 0;

    @Column(name = "status")
    private String status; // PENDING, IN_PROGRESS, COMPLETED, FAILED, SENT, CANCELLED

    @Column(name = "job_type")
    private String jobType; // SYNC, ASYNC, LONG_RUNNING

    @Column(name = "business_domain")
    private String businessDomain;

    @Column(name = "target_batch")
    private String targetBatch;

    @Column(name = "priority")
    private String priority;

    @Column(name = "estimated_duration_minutes")
    private Integer estimatedDurationMinutes;

    @Column(name = "started_at")
    private LocalDateTime startedAt;

    @Column(name = "expected_completion_at")
    private LocalDateTime expectedCompletionAt;

    @CreationTimestamp
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @Column(name = "sent_at")
    private LocalDateTime sentAt;

    @Column(name = "kafka_topic")
    private String kafkaTopic;

    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    // Métodos de ayuda
    public boolean isLongRunning() {
        return estimatedDurationMinutes != null && estimatedDurationMinutes > 30;
    }

    public boolean canRetry() {
        return attempts < 3 && !"FAILED".equals(status) && !"CANCELLED".equals(status);
    }

    public void incrementAttempts() {
        this.attempts = (this.attempts == null ? 0 : this.attempts) + 1;
        this.updatedAt = LocalDateTime.now();
    }
}

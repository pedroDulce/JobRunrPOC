package common.batch.dto;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "job_results")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false, unique = true)
    private String jobId;

    @Column(nullable = false)
    private Boolean success;

    @Column(length = 2000)
    private String message;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @Column(name = "job_type")
    private String jobType;

    @Column(name = "execution_time_ms")
    private Long executionTimeMs;

    public JobResult(String jobId, boolean success, String result, LocalDateTime completedAt) {
        this.completedAt = completedAt;
        this.success = success;
        this.message = result;
        this.jobId = jobId;
    }

    @PrePersist
    public void prePersist() {
        if (completedAt == null) {
            completedAt = LocalDateTime.now();
        }
    }
}

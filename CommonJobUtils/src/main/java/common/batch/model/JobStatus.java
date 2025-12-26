package common.batch.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "job_status")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobStatus {

    @Id
    private String id;  // Usar String para UUID

    @PrePersist
    public void prePersist() {
        if (id == null) {
            id = UUID.randomUUID().toString();
        }
    }

    @Column(name = "job_id", unique = true)
    private String jobId;  // ID externo del job

    private String status;
    private String message;
    private LocalDateTime createdAt;
    private LocalDateTime finishedAt;
    private String jobType;
    private String parametersJson;
}
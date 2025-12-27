package com.company.batchscheduler.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Type;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "job_status")
@Data
@NoArgsConstructor
@Builder
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
    private LocalDateTime updatedAt;
    private LocalDateTime completedAt;
    private String jobType;

    @Column(name = "metadata", columnDefinition = "jsonb")
    @Type(io.hypersistence.utils.hibernate.type.json.JsonBinaryType.class) // Usar JsonBinaryType
    // O alternativamente:
    // @Type(org.hibernate.type.JsonType.class)
    // @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> metadata;
}
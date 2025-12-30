package com.company.batchscheduler.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class JobRunrAdminRepository {

    private static final List<String> STATUS_POSIBLES = List.of("ENQUEUED", "SCHEDULED","PROCESSING",
            "SUCCEEDED", "FAILED", "DELETED");

    private final JdbcTemplate jdbcTemplate;

    public JobRunrAdminRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int updateJobState(UUID jobId, String newState) {
        if (STATUS_POSIBLES.contains(newState)) {
            String sql = """
                UPDATE jobrunr_jobrunr_jobs
                SET state = ?,
                    updatedat = now(),
                    version = version + 1
                WHERE id = ?
            """;

            int updated = jdbcTemplate.update(sql,
                    newState,
                    jobId.toString() // PASAR COMO STRING, no UUID
            );

            if (updated == 0) {
                log.warn("No se encontró job con id {}", jobId);
            } else {
                log.info("Job {} actualizado a estado {}", jobId, newState);
            }
            return updated;
        } else {
            throw new RuntimeException("Estado: " + newState + " no permitido. Solo estados: " + STATUS_POSIBLES.toArray());
        }
    }
}

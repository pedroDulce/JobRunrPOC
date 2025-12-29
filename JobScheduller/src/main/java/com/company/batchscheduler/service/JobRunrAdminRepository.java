package com.company.batchscheduler.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
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
                        SET
                            state = ?,
                            updated_at = now(),
                            version = version + 1
                        WHERE id = ?
                    """;
            return jdbcTemplate.update(sql, newState, jobId);
        }
        throw new RuntimeException("Estado: " + newState + " no permitido. Solo estados: " + STATUS_POSIBLES.toArray());
    }
}

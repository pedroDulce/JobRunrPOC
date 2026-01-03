package com.company.batchscheduler.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class JobRunerRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    /**
     * Actualizar trabajo a estado PROCESSING
     * @param jobId ID del trabajo
     * @return número de filas actualizadas
     */
    public boolean updateJobToProcessing(UUID jobId) {
        // Primero: Bloquear la fila para evitar condiciones de carrera
        String lockSql = "SELECT id FROM jobrunr_jobs WHERE id = ? FOR UPDATE";
        jdbcTemplate.queryForObject(lockSql, UUID.class, jobId);

        String updateSql = """
            UPDATE jobrunr_jobrunr_jobs 
            SET state = 'PROCESSING', 
                updatedAt = ?,
                completedAt = NULL,
                version = version + 1
            WHERE id = ? 
            """;

        int updated = jdbcTemplate.update(updateSql, Instant.now(), jobId);
        return updated > 0;
    }
}


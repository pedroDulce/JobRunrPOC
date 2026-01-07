package com.example.batch.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;

@Configuration
public class BatchSchemaInitializer {

    @Bean
    public Boolean initializeBatchSchema(DataSource dataSource) {
        if (dataSource != null) {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-h2.sql"));
            DatabasePopulatorUtils.execute(populator, dataSource);
        }
        return Boolean.TRUE;
    }
}

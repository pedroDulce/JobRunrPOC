package com.ad.muface.jobs.infra.config;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Primary
    @Bean(name = "businessDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.business")
    public DataSource businessDataSource() {
        return DataSourceBuilder.create().build();
    }

    // DataSource para Spring Batch
    @Bean(name = "batchDataSource")
    @ConfigurationProperties(prefix = "spring.batch.datasource")
    public DataSource batchDataSource() {
        return DataSourceBuilder.create().build();
    }

    // Inicializador del DataSource de batch: crea las tablas
    @Bean
    public DataSourceInitializer batchDataSourceInitializer(@Qualifier("batchDataSource") DataSource batchDataSource,
                                                            ResourceLoader resourceLoader) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(batchDataSource);

        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        // Cargar el script de creación de tablas para PostgreSQL desde el classpath
        populator.addScript(resourceLoader.getResource("classpath:org/springframework/batch/core/schema-postgresql.sql"));
        populator.setContinueOnError(true); // Continuar si las tablas ya existen
        populator.setIgnoreFailedDrops(true);

        initializer.setDatabasePopulator(populator);
        return initializer;
    }

    @Bean
    public JobRepository jobRepository(@Qualifier("batchDataSource") DataSource batchDataSource) throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(batchDataSource);
        factory.setTransactionManager(batchTransactionManager(batchDataSource));
        factory.setDatabaseType("POSTGRES");
        factory.setTablePrefix("BATCH_");
        factory.setMaxVarCharLength(1000);
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    @Bean
    public PlatformTransactionManager batchTransactionManager(@Qualifier("batchDataSource") DataSource batchDataSource) {
        return new DataSourceTransactionManager(batchDataSource);
    }
}

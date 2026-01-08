package com.ad.muface.jobs.infra.config;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class DataSourceConfig {

    /**
     * DataSource PRIMARIO para la base de datos de negocio
     * Se marca como @Primary para que sea el DataSource por defecto
     */
    @Primary
    @Bean(name = "businessDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.business")
    public DataSource businessDataSource() {
        log.info("🔧 Creando businessDataSource...");
        return DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .build();
    }

    /**
     * DataSource para Spring Batch
     */
    @Bean(name = "batchDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.batch")
    public DataSource batchDataSource() {
        log.info("🔧 Creando batchDataSource...");
        return DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .build();
    }

    /**
     * Bean 'dataSource' (nombre requerido por Spring Batch)
     * Este es un ALIAS para batchDataSource
     * Spring Batch internamente busca un bean llamado 'dataSource'
     */
    @Bean(name = "dataSource")
    public DataSource dataSource() {
        log.info("🔧 Creando bean 'dataSource' (alias para batchDataSource)...");
        return batchDataSource();  // Devolvemos el mismo DataSource de batch
    }

    /**
     * Bean 'transactionManager' requerido por Spring Batch.
     * Usará el bean 'dataSource' (que es batchDataSource) para las transacciones de metadatos.
     */
    @Bean(name = "transactionManager")
    @Primary
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        log.info("🔧 Creando transactionManager para Spring Batch...");
        return new DataSourceTransactionManager(dataSource);
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
    public PlatformTransactionManager batchTransactionManager(@Qualifier("batchDataSource") DataSource batchDataSource) {
        return new DataSourceTransactionManager(batchDataSource);
    }
}

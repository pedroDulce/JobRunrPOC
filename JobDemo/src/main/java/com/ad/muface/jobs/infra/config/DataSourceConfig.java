package com.ad.muface.jobs.infra.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    // ==================== Datasource Batch (Spring Batch Metadata) ====================
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource.batch")
    public DataSourceProperties batchDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "dataSource")
    @Primary
    @ConfigurationProperties("spring.datasource.batch.hikari")
    public DataSource batchDataSource(@Qualifier("batchDataSourceProperties") DataSourceProperties props) {
        return props.initializeDataSourceBuilder().build();
    }

    @Bean(name = "transactionManager")
    @Primary
    public DataSourceTransactionManager batchTransactionManager(@Qualifier("dataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    // Si necesitas uno para JDBC puro sobre la base de negocio
    @Bean(name = "businessJdbcTransactionManager")
    public DataSourceTransactionManager businessJdbcTransactionManager(@Qualifier("businessDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    // ==================== Datasource de negocio ====================
    @Bean
    @ConfigurationProperties("spring.datasource.business")
    public DataSourceProperties businessDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "businessDataSource")
    @ConfigurationProperties("spring.datasource.business.hikari")
    public DataSource businessDataSource(@Qualifier("businessDataSourceProperties") DataSourceProperties props) {
        return props.initializeDataSourceBuilder().build();
    }

}


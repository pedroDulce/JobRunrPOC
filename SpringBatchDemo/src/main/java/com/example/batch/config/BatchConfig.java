package com.example.batch.config;

import com.example.batch.model.Persona;
import com.example.batch.processor.PersonaItemProcessor;
import com.example.monitor.StepMetricsListener;

import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
    private final MeterRegistry meterRegistry;

    public BatchConfig(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Bean
    public StepMetricsListener stepMetricsListener() {
        return new StepMetricsListener(meterRegistry);
    }

    @Bean
    @org.springframework.lang.NonNull
    public FlatFileItemReader<Persona> reader() {
        FlatFileItemReader<Persona> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("personas.csv"));
        reader.setLinesToSkip(1);

        DefaultLineMapper<Persona> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("nombre", "apellido", "edad");

        BeanWrapperFieldSetMapper<Persona> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(Persona.class);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);
        reader.setLineMapper(lineMapper);
       
        
        return reader;
    }

    @Bean
    @org.springframework.lang.NonNull
    public PersonaItemProcessor processor() {
        return new PersonaItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Persona> writer(@org.springframework.lang.NonNull DataSource dataSource) {
        JdbcBatchItemWriter<Persona> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO persona (nombre, apellido, edad) VALUES (:nombre, :apellido, :edad)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

        @Bean
        public Step importStep(@org.springframework.lang.NonNull JobRepository jobRepository,
                   @org.springframework.lang.NonNull PlatformTransactionManager transactionManager,
                   @org.springframework.lang.NonNull FlatFileItemReader<Persona> reader,
                   @org.springframework.lang.NonNull JdbcBatchItemWriter<Persona> writer,
                @org.springframework.lang.NonNull StepMetricsListener stepMetricsListener) {
    
            return new StepBuilder("importStep", jobRepository)
                .<Persona, Persona>chunk(5, transactionManager)
                .reader(reader)
                .processor(processor())
                .writer(writer)
                .listener(stepMetricsListener)  // <-- Listener que mide duración
                .build();
            }

        @Bean
        public Job importPersonasJob(@org.springframework.lang.NonNull JobRepository jobRepository, @org.springframework.lang.NonNull Step importStep) {
        return new JobBuilder("importPersonasJob", jobRepository)
            .start(importStep)
            .build();
        }
}

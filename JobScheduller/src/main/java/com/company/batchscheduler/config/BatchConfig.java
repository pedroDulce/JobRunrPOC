package com.company.batchscheduler.config;

import com.company.batchscheduler.model.CustomerTransaction;
import com.company.batchscheduler.model.DailySummary;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchConfig {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Value("${app.batch.chunk-size:100}")
    private int chunkSize;

    @Bean
    public JdbcCursorItemReader<CustomerTransaction> transactionReader() {
        return new JdbcCursorItemReaderBuilder<CustomerTransaction>()
                .name("transactionReader")
                .dataSource(dataSource)
                .sql("""
                SELECT id, transaction_id, customer_id, amount, 
                       currency, transaction_date, status, source_file, created_at
                FROM customer_transactions 
                WHERE status = 'ENQUEUED' 
                AND transaction_date = ? 
                ORDER BY customer_id
                """)
                .rowMapper(new BeanPropertyRowMapper<>(CustomerTransaction.class))
                .preparedStatementSetter(ps -> {
                    // La fecha se inyecta desde JobParameters
                    ps.setDate(1, java.sql.Date.valueOf(LocalDate.now().minusDays(1)));
                })
                .fetchSize(chunkSize)
                .build();
    }

    @Bean
    public ItemProcessor<CustomerTransaction, DailySummary> summaryProcessor() {
        return transaction -> {
            DailySummary summary = new DailySummary();
            summary.setCustomerId(transaction.getCustomerId());
            summary.setSummaryDate(transaction.getTransactionDate());
            summary.setCurrency(transaction.getCurrency());
            summary.setTotalAmount(transaction.getAmount());
            summary.setTransactionCount(1);

            // Podríamos agregar lógica de negocio aquí
            // Ej: validaciones, transformaciones, cálculos adicionales

            return summary;
        };
    }

    @Bean
    public ItemWriter<DailySummary> summaryWriter() {
        return summaries -> {
            // Agrupar por cliente y fecha
            Map<String, DailySummary> grouped = new HashMap<>();

            for (DailySummary summary : summaries) {
                String key = summary.getCustomerId() + "_" +
                        summary.getSummaryDate() + "_" +
                        summary.getCurrency();

                if (grouped.containsKey(key)) {
                    DailySummary existing = grouped.get(key);
                    existing.setTotalAmount(
                            existing.getTotalAmount().add(summary.getTotalAmount())
                    );
                    existing.setTransactionCount(
                            existing.getTransactionCount() + 1
                    );
                } else {
                    grouped.put(key, summary);
                }
            }

            // Aquí iría el writer real a base de datos
            // Por simplicidad, lo mostramos en logs
            grouped.values().forEach(s ->
                    System.out.printf("Resumen: Cliente %s, Total: %s %s, Transacciones: %d%n",
                            s.getCustomerId(), s.getTotalAmount(), s.getCurrency(), s.getTransactionCount())
            );
        };
    }

    @Bean
    public Step processTransactionsStep(
            ItemReader<CustomerTransaction> reader,
            ItemProcessor<CustomerTransaction, DailySummary> processor,
            ItemWriter<DailySummary> writer) {

        return new StepBuilder("processTransactionsStep", jobRepository)
                .<CustomerTransaction, DailySummary>chunk(chunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Job dailySummaryJob(Step processTransactionsStep) {
        return new JobBuilder("dailySummaryJob", jobRepository)
                .start(processTransactionsStep)
                .build();
    }
}


package org.example.batch.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.batch.model.CustomerTransaction;
import org.example.batch.model.ProcessedTransaction;
import org.example.batch.service.BatchStatusNotifier;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class SpringBatchExecutorConfig {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchStatusNotifier batchStatusNotifier; // Servicio para notificar estados

    @Value("${app.batch.chunk-size:100}")
    private int chunkSize;

    @Value("${app.batch.grid-size:4}")
    private int gridSize;

    @Value("${app.batch.partition-size:1000}")
    private int partitionSize;

    // ============= PARTITIONER =============
    @Bean
    public Partitioner transactionPartitioner() {
        return gridSize -> {
            Map<String, ExecutionContext> partitions = new HashMap<>();
            LocalDate processDate = LocalDate.now().minusDays(1);

            for (int i = 0; i < gridSize; i++) {
                ExecutionContext context = new ExecutionContext();
                context.put("partitionNumber", i);
                context.put("startId", i * partitionSize);
                context.put("endId", (i + 1) * partitionSize - 1);
                context.put("processDate", processDate);

                partitions.put("partition-" + i, context);
            }

            log.info("Creadas {} particiones para procesamiento paralelo", gridSize);
            return partitions;
        };
    }

    // ============= READER POR PARTICIÓN =============
    @Bean
    @StepScope
    public JdbcCursorItemReader<CustomerTransaction> partitionedTransactionReader(
            @Value("#{stepExecutionContext['startId']}") Long startId,
            @Value("#{stepExecutionContext['endId']}") Long endId,
            @Value("#{stepExecutionContext['processDate']}") LocalDate processDate) {

        return new JdbcCursorItemReaderBuilder<CustomerTransaction>()
                .name("partitionedTransactionReader")
                .dataSource(dataSource)
                .sql("""
                    SELECT id, transaction_id, customer_id, amount, 
                           currency, transaction_date, status, source_file, created_at
                    FROM customer_transactions 
                    WHERE status = 'PENDING'
                    AND transaction_date = ?
                    AND id BETWEEN ? AND ?
                    ORDER BY id
                    """)
                .rowMapper(new BeanPropertyRowMapper<>(CustomerTransaction.class))
                .preparedStatementSetter(ps -> {
                    ps.setDate(1, java.sql.Date.valueOf(processDate));
                    ps.setLong(2, startId);
                    ps.setLong(3, endId);
                })
                .fetchSize(chunkSize)
                .build();
    }

    // ============= PROCESSOR =============
    @Bean
    @StepScope
    public ItemProcessor<CustomerTransaction, ProcessedTransaction> transactionProcessor(
            @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {

        return transaction -> {
            // Aquí va tu lógica de procesamiento
            ProcessedTransaction processed = new ProcessedTransaction();
            processed.setTransactionId(transaction.getTransactionId());
            processed.setCustomerId(transaction.getCustomerId());
            processed.setAmount(transaction.getAmount());
            processed.setCurrency(transaction.getCurrency());
            processed.setStatus("PROCESSED");
            processed.setPartitionNumber(partitionNumber);

            return processed;
        };
    }

    // ============= WRITER =============
    @Bean
    public ItemWriter<ProcessedTransaction> transactionWriter() {
        return transactions -> {
            // Writer que procesa el chunk completo
            log.info("Escribiendo lote de {} transacciones procesadas", transactions.size());

            // Aquí normalmente guardarías en BD o enviarías a otro sistema
            // Por ahora solo logueamos
            transactions.forEach(t ->
                    log.debug("Transacción procesada: {}", t.getTransactionId())
            );
        };
    }

    // ============= STEP DEL WORKER =============
    @Bean
    @StepScope
    public Step workerStep(
            ItemReader<CustomerTransaction> reader,
            ItemProcessor<CustomerTransaction, ProcessedTransaction> processor,
            ItemWriter<ProcessedTransaction> writer,
            @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {

        return new StepBuilder("workerStep-" + partitionNumber, jobRepository)
                .<CustomerTransaction, ProcessedTransaction>chunk(chunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        log.info("🚀 Iniciando partición {}", partitionNumber);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        log.info("✅ Partición {} completada. Procesados: {}",
                                partitionNumber, stepExecution.getWriteCount());
                        return ExitStatus.COMPLETED;
                    }
                })
                .build();
    }

    // ============= PARTITION HANDLER =============
    @Bean
    public PartitionHandler partitionHandler() {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setTaskExecutor(batchTaskExecutor());
        handler.setStep(workerStep(null, null, null, 0)); // Dummy para inicialización
        handler.setGridSize(gridSize);
        return handler;
    }

    // ============= STEP MASTER =============
    @Bean
    public Step masterStep(Partitioner partitioner, PartitionHandler partitionHandler) {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("workerStep", partitioner)
                .partitionHandler(partitionHandler)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        // Extraer jobId del contexto y notificar inicio
                        String jobId = stepExecution.getJobParameters().getString("externalJobId");
                        if (jobId != null) {
                            batchStatusNotifier.notifyProgress(
                                    jobId, "PROGRESS", "Iniciando procesamiento batch", 0
                            );
                        }
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        // Notificar progreso
                        String jobId = stepExecution.getJobParameters().getString("externalJobId");
                        if (jobId != null) {
                            int progress = (int) ((stepExecution.getWriteCount() * 100) /
                                    Math.max(1, stepExecution.getReadCount()));
                            batchStatusNotifier.notifyProgress(
                                    jobId, "PROGRESS", "Procesando...", progress
                            );
                        }
                        return ExitStatus.COMPLETED;
                    }
                })
                .build();
    }

    // ============= TASK EXECUTOR =============
    @Bean
    public TaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(gridSize);
        executor.setMaxPoolSize(gridSize * 2);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("batch-worker-");
        executor.initialize();
        return executor;
    }

    // ============= JOB PRINCIPAL =============
    @Bean
    public Job dailyTransactionBatchJob(Step masterStep) {
        return new JobBuilder("dailyTransactionBatchJob", jobRepository)
                .start(masterStep)
                .listener(batchJobExecutionListener())
                .build();
    }

    // ============= LISTENER PARA NOTIFICACIONES =============
    @Bean
    public JobExecutionListener batchJobExecutionListener() {
        return new JobExecutionListener() {
            private final BatchStatusNotifier notifier = batchStatusNotifier;

            @Override
            public void beforeJob(JobExecution jobExecution) {
                String jobId = jobExecution.getJobParameters().getString("externalJobId");
                log.info("🚀 Iniciando batch job para job externo: {}", jobId);

                // Notificar INICIO al Job Scheduler
                if (jobId != null) {
                    notifier.notifyStart(jobId, "Batch job iniciado");
                }
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                String jobId = jobExecution.getJobParameters().getString("externalJobId");

                if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                    log.info("✅ Batch job completado exitosamente para: {}", jobId);

                    // Generar informe
                    Map<String, Object> report = generateReport(jobExecution);

                    // Notificar COMPLETADO con informe
                    if (jobId != null) {
                        notifier.notifyCompletion(
                                jobId,
                                "SUCCEEDED",
                                "Procesamiento batch completado",
                                report
                        );

                        // Enviar email con informe
                        notifier.sendEmailReport(jobId, report);
                    }

                } else {
                    log.error("❌ Batch job falló para: {}", jobId);

                    if (jobId != null) {
                        notifier.notifyCompletion(
                                jobId,
                                "FAILED",
                                "Error en procesamiento batch",
                                null
                        );
                    }
                }
            }

            private Map<String, Object> generateReport(JobExecution execution) {
                Map<String, Object> report = new HashMap<>();
                report.put("jobExecutionId", execution.getId());
                report.put("jobName", execution.getJobInstance().getJobName());
                report.put("status", execution.getStatus().toString());
                report.put("startTime", execution.getStartTime());
                report.put("endTime", execution.getEndTime());
                report.put("readCount", execution.getStepExecutions().stream()
                        .mapToLong(StepExecution::getReadCount).sum());
                report.put("writeCount", execution.getStepExecutions().stream()
                        .mapToLong(StepExecution::getWriteCount).sum());
                report.put("partitions", execution.getStepExecutions().size());
                return report;
            }
        };
    }
}
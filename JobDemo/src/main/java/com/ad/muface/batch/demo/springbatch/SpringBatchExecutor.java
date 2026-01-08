package com.ad.muface.batch.demo.springbatch;

import com.ad.muface.batch.demo.model.CustomerTransaction;
import com.ad.muface.batch.demo.model.ProcessedTransaction;
import com.ad.muface.batch.dto.JobStatusEnum;
import com.ad.muface.batch.notifier.EmailReporter;
import com.ad.muface.batch.notifier.KafkaPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import org.springframework.beans.factory.annotation.Qualifier;
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
public class SpringBatchExecutor {

    @Qualifier("businessDataSource")
    private final DataSource businessDataSource;

    private final EmailReporter emailReporter;
    private final KafkaPublisher notifierProgress;

    @Value("${app.batch.chunk-size:100}")
    private int chunkSize;

    @Value("${app.batch.grid-size:4}")
    private int gridSize;

    @Value("${app.batch.partition-size:1000}")
    private int partitionSize;


    @Bean
    public Partitioner transactionPartitioner(@Value("#{jobParameters['processDate']}") String processDateParam,
                                              @Value("#{jobParameters['emailRecipient']}") String emailRecipient,
                                              @Value("#{jobParameters['customerFilter']}") String customerFilter) {
        return new Partitioner() {
            @Override
            public Map<String, ExecutionContext> partition(int gridSize) {
                Map<String, ExecutionContext> partitions = new HashMap<>();
                LocalDate processDate = LocalDate.parse(processDateParam);
                log.info("procesando trabajo con los jobparameters recibidos de la JobRequest: ");
                log.info("processDateParam: " + processDateParam);
                log.info("emailRecipient: " + emailRecipient);
                log.info("customerFilter: " + customerFilter);

                for (int i = 0; i < gridSize; i++) {
                    ExecutionContext context = new ExecutionContext();
                    context.putLong("startId", (long) i * partitionSize);
                    context.putLong("endId", (long) ((i + 1) * partitionSize - 1));
                    context.putInt("partitionNumber", i);
                    context.put("processDate", processDate);
                    context.put("emailRecipient", emailRecipient);
                    context.put("customerFilter", customerFilter);
                    partitions.put("partition-" + i, context);
                }
                return partitions;
            }
        };
    }


    // ================= READER =================
    @Bean
    @StepScope
    public JdbcCursorItemReader<CustomerTransaction> partitionedTransactionReader(
            @Value("#{stepExecutionContext['startId']}") Long startId,
            @Value("#{stepExecutionContext['endId']}") Long endId,
            @Value("#{stepExecutionContext['processDate']}") LocalDate processDate) {
        log.debug("processDate : " + java.sql.Date.valueOf(processDate));
        return new JdbcCursorItemReaderBuilder<CustomerTransaction>()
                .name("partitionedTransactionReader")
                .dataSource(businessDataSource)
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

    // ================= PROCESSOR =================
    @Bean
    @StepScope
    public ItemProcessor<CustomerTransaction, ProcessedTransaction> transactionProcessor(
            @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {

        return tx -> {
            ProcessedTransaction processed = new ProcessedTransaction();
            processed.setTransactionId(tx.getTransactionId());
            processed.setCustomerId(tx.getCustomerId());
            processed.setAmount(tx.getAmount());
            processed.setCurrency(tx.getCurrency());
            processed.setStatus("PROCESSED");
            processed.setPartitionNumber(partitionNumber);
            return processed;
        };
    }

    // ================= WRITER =================
    @Bean
    public ItemWriter<ProcessedTransaction> transactionWriter() {
        return items -> log.info("Escribiendo {} transacciones procesadas", items.size());
    }

    @Bean
    public Step workerStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemReader<CustomerTransaction> partitionedTransactionReader,  // Cambiado a interfaz
            ItemProcessor<CustomerTransaction, ProcessedTransaction> transactionProcessor,
            ItemWriter<ProcessedTransaction> transactionWriter) {

        return new StepBuilder("workerStep", jobRepository)
                .<CustomerTransaction, ProcessedTransaction>chunk(chunkSize, transactionManager)
                .reader(partitionedTransactionReader)  // Usar directamente
                .processor(transactionProcessor)
                .writer(transactionWriter)
                .build();
    }

    // ================= PARTITION HANDLER =================
    @Bean
    public PartitionHandler partitionHandler(Step workerStep, TaskExecutor batchTaskExecutor) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setTaskExecutor(batchTaskExecutor);
        handler.setStep(workerStep);
        handler.setGridSize(gridSize);
        return handler;
    }

    // ================= MASTER STEP =================
    @Bean
    public Step masterStep(
            JobRepository jobRepository,
            Partitioner transactionPartitioner,
            PartitionHandler partitionHandler) {

        return new StepBuilder("masterStep", jobRepository)
                .partitioner("workerStep", transactionPartitioner)
                .partitionHandler(partitionHandler)
                .build();
    }

    // ================= JOB =================
    @Bean
    public Job dailyTransactionBatchJob(
            JobRepository jobRepository,
            Step masterStep) {

        return new JobBuilder("dailyTransactionBatchJob", jobRepository)
                .start(masterStep)
                .listener(batchJobExecutionListener())
                .build();
    }

    // ================= TASK EXECUTOR =================
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

    // ================= JOB LISTENER =================
    @Bean
    public JobExecutionListener batchJobExecutionListener() {
        return new JobExecutionListener() {

            @Override
            public void beforeJob(JobExecution jobExecution) {
                String jobId = jobExecution.getJobParameters().getString("externalJobId");
                if (jobId != null) {
                    notifierProgress.notifyStart(jobExecution.getJobParameters().getString("jobId"),
                            jobExecution.getJobParameters().getString("jobname"),
                            jobExecution.getJobParameters().getString("correlationId"),
                            "Batch job iniciado", jobExecution);
                }
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                String jobId = jobExecution.getJobParameters().getString("externalJobId");

                if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                    Map<String, Object> report = new HashMap<>();
                    report.put("readCount", jobExecution.getStepExecutions()
                            .stream().mapToLong(StepExecution::getReadCount).sum());
                    report.put("writeCount", jobExecution.getStepExecutions()
                            .stream().mapToLong(StepExecution::getWriteCount).sum());

                    try {
                        log.debug("Simulamos carga de procesamiento elevada antes de devolver el control al scheduler...");
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    notifierProgress.notifyCompletion(
                            jobExecution.getJobParameters().getString("jobId"),
                            jobExecution.getJobParameters().getString("jobname"),
                            jobExecution.getJobParameters().getString("correlationId"),
                            "Batch completado con éxito",
                            report, jobExecution);

                    emailReporter.sendEmailReport(jobId, report);

                } else {
                    notifierProgress.notifyFailure(jobExecution.getJobParameters().getString("jobId"),
                            jobExecution.getJobParameters().getString("jobname"),
                            jobExecution.getJobParameters().getString("correlationId"),
                            "Batch completado con éxito", jobExecution);
                }
            }
        };
    }
}

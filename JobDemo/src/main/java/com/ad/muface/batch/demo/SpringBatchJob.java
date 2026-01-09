package com.ad.muface.batch.demo;

import com.ad.muface.batch.demo.model.CustomerTransaction;
import com.ad.muface.batch.demo.model.ProcessedTransaction;
import com.ad.muface.batch.notifier.EmailReporter;
import com.ad.muface.batch.notifier.KafkaPublisher;
import com.ad.muface.batch.service.HeartbeatService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class SpringBatchJob {
    @Qualifier("businessDataSource")
    private final DataSource businessDataSource;
    private final HeartbeatService heartbeatService;
    private final EmailReporter emailReporter;
    private final KafkaPublisher notifierProgress;
    @Value("${app.batch.chunk-size:100}")
    private int chunkSize;

    @Value("${app.batch.grid-size:4}")
    private int gridSize;

    @Value("${app.batch.partition-size:1000}")
    private int partitionSize;

    // Añade este bean para deshabilitar auto-ejecución
    @Bean
    public ApplicationRunner disableBatchAutoStart(JobLauncher jobLauncher, JobRepository jobRepository) {
        return args -> {
            log.info("Batch auto-start deshabilitado. Los jobs se ejecutarán manualmente.");
        };
    }

    // ================= PARTITIONER CORREGIDO =================
    @Bean
    @StepScope
    public Partitioner transactionPartitioner(
            @Value("#{jobParameters['processDate'] ?: T(java.time.LocalDate).now().minusDays(1).toString()}") String processDateParam,
            @Value("#{jobParameters['emailRecipient'] ?: 'default@example.com'}") String emailRecipient,
            @Value("#{jobParameters['customerFilter'] ?: ''}") String customerFilter) {

        return new Partitioner() {
            @Override
            public Map<String, ExecutionContext> partition(int gridSize) {
                Map<String, ExecutionContext> partitions = new HashMap<>();

                // Manejar posibles errores en la fecha
                LocalDate processDate;
                try {
                    processDate = LocalDate.parse(processDateParam);
                } catch (DateTimeParseException e) {
                    log.error("Error parseando processDate: {}, usando fecha por defecto", processDateParam, e);
                    processDate = LocalDate.now().minusDays(1);
                }

                log.info("=== Iniciando particiones ===");
                log.info("Fecha de proceso: {}", processDate);
                log.info("Email destinatario: {}", emailRecipient);
                log.info("Filtro cliente: {}", customerFilter);
                log.info("Grid size: {}", gridSize);
                log.info("Tamaño de partición: {}", partitionSize);

                // CALCULAR RANGOS REALES BASADOS EN LA BASE DE DATOS
                // Primero, obtener el ID mínimo y máximo para la fecha
                Long minId = getMinIdForDate(processDate);
                Long maxId = getMaxIdForDate(processDate);

                if (minId == null || maxId == null) {
                    log.warn("No se encontraron registros para la fecha: {}", processDate);
                    return partitions; // Retorna mapa vacío
                }

                log.info("ID mínimo encontrado: {}, ID máximo: {}", minId, maxId);
                log.info("Total de registros estimados: {}", (maxId - minId + 1));

                // Calcular tamaño de partición basado en los IDs reales
                long totalIds = maxId - minId + 1;
                long idsPerPartition = (totalIds + gridSize - 1) / gridSize; // División redondeando hacia arriba

                for (int i = 0; i < gridSize; i++) {
                    long startId = minId + (i * idsPerPartition);
                    long endId = Math.min(startId + idsPerPartition - 1, maxId);

                    // Si startId > maxId, no hay más registros
                    if (startId > maxId) {
                        break;
                    }

                    ExecutionContext context = new ExecutionContext();
                    context.putLong("startId", startId);
                    context.putLong("endId", endId);
                    context.putInt("partitionNumber", i);
                    context.put("processDate", processDate);
                    context.put("emailRecipient", emailRecipient);
                    context.put("customerFilter", customerFilter);
                    context.put("numberOfPartitions", partitions.size());
                    partitions.put("partition-" + i, context);

                    log.info("Partición {}: IDs {} - {}", i, startId, endId);
                }

                log.info("Total de particiones creadas: {}", partitions.size());
                return partitions;
            }

            private Long getMinIdForDate(LocalDate date) {

                String sql = "SELECT MIN(id) FROM customer_transactions WHERE status = 'PENDING' AND transaction_date = ?";

                try {
                    JdbcTemplate businessJdbcTemplate = new JdbcTemplate(businessDataSource);

                    // queryForObject puede lanzar EmptyResultDataAccessException si no hay resultados
                    Long minId = businessJdbcTemplate.queryForObject(sql, Long.class, java.sql.Date.valueOf(date));

                    // También puede devolver null si la columna es nullable (aunque id no lo es)
                    if (minId == null) {
                        log.warn("No se encontró ID mínimo para fecha {} (queryForObject devolvió null)", date);
                        return null;
                    }

                    log.debug("ID mínimo encontrado para fecha {}: {}", date, minId);
                    return minId;

                } catch (EmptyResultDataAccessException e) {
                    // Esto ocurre cuando la consulta no devuelve filas
                    log.warn("No se encontraron registros PENDING para la fecha {} (consulta vacía)", date);
                    return null;
                } catch (Exception e) {
                    log.error("Error obteniendo ID mínimo para fecha {}", date, e);
                    return null;
                }
            }

            private Long getMaxIdForDate(LocalDate date) {

                String sql = "SELECT MAX(id) FROM customer_transactions WHERE status = 'PENDING' AND transaction_date = ?";

                try {
                    JdbcTemplate businessJdbcTemplate = new JdbcTemplate(businessDataSource);
                    Long maxId = businessJdbcTemplate.queryForObject(sql, Long.class, java.sql.Date.valueOf(date));

                    if (maxId == null) {
                        log.warn("No se encontró ID máximo para fecha {} (queryForObject devolvió null)", date);
                        return null;
                    }

                    log.debug("ID máximo encontrado para fecha {}: {}", date, maxId);
                    return maxId;

                } catch (EmptyResultDataAccessException e) {
                    log.warn("No se encontraron registros PENDING para la fecha {} (consulta vacía)", date);
                    return null;
                } catch (Exception e) {
                    log.error("Error obteniendo ID máximo para fecha {}", date, e);
                    return null;
                }
            }
        };
    }

    // ================= READER CORREGIDO =================
    @Bean
    @StepScope
    public JdbcCursorItemReader<CustomerTransaction> partitionedTransactionReader(
            @Value("#{stepExecutionContext['startId']}") Long startId,
            @Value("#{stepExecutionContext['endId']}") Long endId,
            @Value("#{stepExecutionContext['processDate']}") LocalDate processDate) {

        log.info("=== Creando Reader ===");
        log.info("Fecha: {}", processDate);
        log.info("Rango de IDs: {} - {}", startId, endId);

        // REMOVER EL Thread.sleep - Es crítico para el rendimiento
        // try {
        //     log.debug("Simulamos carga de procesamiento elevada antes de devolver el control al scheduler...");
        //     Thread.sleep(30000);  // ¡QUITAR ESTO!
        // } catch (InterruptedException e) {
        //     throw new RuntimeException(e);
        // }

        // Primero, verificar si hay registros para este rango
        Long count = getRecordCount(processDate, startId, endId);
        log.info("Registros encontrados en este rango: {}", count);

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
                    log.debug("Estableciendo parámetros: date={}, start={}, end={}",
                            processDate, startId, endId);
                    ps.setDate(1, java.sql.Date.valueOf(processDate));
                    ps.setLong(2, startId);
                    ps.setLong(3, endId);
                })
                .fetchSize(chunkSize)
                .build();
    }

    private Long getRecordCount(LocalDate date, Long startId, Long endId) {
        try {
            JdbcTemplate businessJdbcTemplate = new JdbcTemplate(businessDataSource);
            return businessJdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM customer_transactions " +
                            "WHERE status = 'PENDING' " +
                            "AND transaction_date = ? " +
                            "AND id BETWEEN ? AND ?",
                    Long.class,
                    java.sql.Date.valueOf(date),  // Primer parámetro: date
                    startId,                      // Segundo parámetro: startId
                    endId                         // Tercer parámetro: endId
            );
        } catch (Exception e) {
            log.error("Error contando registros para fecha: {}, IDs: {} - {}",
                    date, startId, endId, e);
            return 0L;
        }
    }

    // ================= PROCESSOR =================
    @Bean
    @StepScope
    public ItemProcessor<CustomerTransaction, ProcessedTransaction> transactionProcessor(
            @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {

        return tx -> {
            log.debug("Procesando transacción: {}", tx.getTransactionId());
            ProcessedTransaction processed = new ProcessedTransaction();
            processed.setTransactionId(tx.getTransactionId());
            processed.setCustomerId(tx.getCustomerId());
            processed.setAmount(tx.getAmount());
            processed.setCurrency(tx.getCurrency());
            processed.setStatus("PROCESSED");
            processed.setPartitionNumber(partitionNumber);
            processed.setOriginalStatus(tx.getStatus());
            processed.setTransactionDate(tx.getTransactionDate());
            return processed;
        };
    }

    // ================= WRITER MEJORADO =================
    @Bean
    @StepScope
    public ItemWriter<ProcessedTransaction> transactionWriter(
            @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {

        return items -> {
            if (!items.isEmpty()) {
                log.info("Partición {}: Escribiendo {} transacciones procesadas",
                        partitionNumber, items.size());
                // Aquí iría la lógica real de escritura (BD, archivo, etc.)
                // Por ahora solo logueamos
                items.forEach(item ->
                        log.debug("Transacción procesada: {} -> {}",
                                item.getTransactionId(), item.getStatus()));
            } else {
                log.info("Partición {}: No hay transacciones para escribir", partitionNumber);
            }
        };
    }


    @Bean
    public Step workerStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemReader<CustomerTransaction> partitionedTransactionReader,
            ItemProcessor<CustomerTransaction, ProcessedTransaction> transactionProcessor,
            ItemWriter<ProcessedTransaction> transactionWriter) {

        return new StepBuilder("workerStep", jobRepository)
                .<CustomerTransaction, ProcessedTransaction>chunk(chunkSize, transactionManager)
                .reader(partitionedTransactionReader)
                .processor(transactionProcessor)
                .writer(transactionWriter)
                .listener(new ItemReadListener<CustomerTransaction>() {
                    @Override
                    public void beforeRead() {
                        // No op
                    }

                    @Override
                    public void afterRead(CustomerTransaction item) {
                        log.debug("Leída transacción: {}", item.getTransactionId());
                    }

                    @Override
                    public void onReadError(Exception ex) {
                        log.error("Error leyendo transacción", ex);
                    }
                })
                .listener(new ItemProcessListener<CustomerTransaction, ProcessedTransaction>() {
                    @Override
                    public void beforeProcess(CustomerTransaction item) {
                        // No op
                    }

                    @Override
                    public void afterProcess(CustomerTransaction item, ProcessedTransaction result) {
                        log.debug("Procesada transacción: {} -> {}",
                                item.getTransactionId(), result.getStatus());
                    }

                    @Override
                    public void onProcessError(CustomerTransaction item, Exception e) {
                        log.error("Error procesando transacción: {}", item.getTransactionId(), e);
                    }
                })
                .listener(new ItemWriteListener<ProcessedTransaction>() {
                    @Override
                    public void beforeWrite(Chunk<? extends ProcessedTransaction> items) {
                        log.debug("...voy a escribir {} transacciones", items.size());
                    }

                    @Override
                    public void afterWrite(Chunk<? extends ProcessedTransaction> items) {
                        log.debug("...Escritas {} transacciones", items.size());
                    }

                    @Override
                    public void onWriteError(Exception exception, Chunk<? extends ProcessedTransaction> items) {
                        log.error("Error escribiendo {} transacciones", items.size(), exception);
                    }
                })
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
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        log.info("=== INICIANDO STEP MASTER ===");
                        log.info("Job Parameters: {}", stepExecution.getJobParameters());
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        log.info("=== FINALIZANDO STEP MASTER ===");
                        log.info("Total leídos: {}", stepExecution.getReadCount());
                        log.info("Total escritos: {}", stepExecution.getWriteCount());
                        log.info("Estado: {}", stepExecution.getStatus());
                        return stepExecution.getExitStatus();
                    }
                })
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
                String jobName = jobExecution.getJobParameters().getString("jobName");
                String correlationId = jobExecution.getJobParameters().getString("jobCorrelationId");

                log.info("=== INICIANDO JOB ===");
                log.info("Job ID: {}", jobId);
                log.info("Job Name: {}", jobName);
                log.info("Correlation ID: {}", correlationId);
                log.info("Job Parameters: {}", jobExecution.getJobParameters());

                if (jobId != null) {
                    notifierProgress.notifyStart(jobId, jobName, correlationId,
                            "Batch job iniciado", jobExecution);
                    heartbeatService.startHeartbeat(jobId, jobName, correlationId);
                }
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                String jobId = jobExecution.getJobParameters().getString("externalJobId");
                String jobName = jobExecution.getJobParameters().getString("jobName");
                String correlationId = jobExecution.getJobParameters().getString("jobCorrelationId");
                long duracionMs = Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();

                log.info("=== FINALIZANDO JOB ===");
                log.info("Estado final: {}", jobExecution.getStatus());
                log.info("Tiempo de ejecución: {}ms", duracionMs);

                heartbeatService.stopHeartbeat(jobId);

                if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                    Map<String, Object> report = new HashMap<>();
                    long readCount = jobExecution.getStepExecutions()
                            .stream().mapToLong(StepExecution::getReadCount).sum();
                    long writeCount = jobExecution.getStepExecutions()
                            .stream().mapToLong(StepExecution::getWriteCount).sum();

                    report.put("readCount", readCount);
                    report.put("writeCount", writeCount);
                    report.put("executionTime", duracionMs);
                    report.put("status", "COMPLETED");
                    report.put("partitionsGridSize", gridSize);
                    log.info("Job completado exitosamente. Leídos: {}, Escritos: {}",
                            readCount, writeCount);

                    notifierProgress.notifyCompletion(jobId, jobName, correlationId,
                            "Batch completado con éxito", report, duracionMs);

                    if (emailReporter != null && readCount > 0) {
                        emailReporter.sendEmailReport(jobId, report);
                    }

                } else {
                    log.error("Job fallido. Estado: {}", jobExecution.getStatus());
                    log.error("Errores: {}", jobExecution.getAllFailureExceptions());

                    notifierProgress.notifyFailure(jobId, jobName, correlationId,
                            "Batch fallido", duracionMs);
                }
            }
        };
    }
}

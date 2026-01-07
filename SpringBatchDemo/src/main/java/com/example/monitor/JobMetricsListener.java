package com.example.monitor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Component
public class JobMetricsListener implements JobExecutionListener {

    private final MeterRegistry meterRegistry;
    private Timer.Sample timerSample;

    public JobMetricsListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void beforeJob(@NonNull JobExecution jobExecution) {
        timerSample = Timer.start(meterRegistry);
    }

    @Override
    public void afterJob(@NonNull JobExecution jobExecution) {

        timerSample.stop(Timer.builder("batch.job.duration")
                .tag("job", jobExecution.getJobInstance().getJobName())
                .tag("status", jobExecution.getStatus().name())
                .register(meterRegistry));

        Counter.builder("batch.job.executions")
                .tag("job", jobExecution.getJobInstance().getJobName())
                .tag("status", jobExecution.getStatus().name())
                .register(meterRegistry)
                .increment();

        long totalWritten = jobExecution.getStepExecutions()
                .stream()
                .mapToLong(StepExecution::getWriteCount)
                .sum();

        meterRegistry.gauge(
                "batch.job.items.processed",
                Tags.of("job", jobExecution.getJobInstance().getJobName()),
                totalWritten
        );
    }
}

package com.example.monitor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

import java.time.LocalDateTime;
import java.time.ZoneId;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Component
public class StepMetricsListener implements StepExecutionListener {

    private final MeterRegistry meterRegistry;
    private Timer.Sample timerSample;

    public StepMetricsListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void beforeStep(@NonNull StepExecution stepExecution) {
        timerSample = Timer.start(meterRegistry);
        System.out.println("Step '" + stepExecution.getStepName() + "' start: " + stepExecution.getStartTime());
    }

    @Override
    public ExitStatus afterStep(@NonNull StepExecution stepExecution) {

        Timer timer = Timer.builder("batch.step.duration")
                .description("Duración de cada Step de Spring Batch")
                .tag("job", stepExecution.getJobExecution().getJobInstance().getJobName())
                .tag("step", stepExecution.getStepName())
                .tag("status", stepExecution.getStatus().name())
                .register(meterRegistry);

        // Detener el timer y registrar la duración
         timerSample.stop(timer);
LocalDateTime start = stepExecution.getStartTime();
LocalDateTime end = stepExecution.getEndTime();

if (start != null && end != null) {
    long durationMs = Duration.between(start.atZone(ZoneId.systemDefault()).toInstant(),
                                       end.atZone(ZoneId.systemDefault()).toInstant())
                           .toMillis();
    System.out.println("Step duration real: " + durationMs + " ms");
}

        return stepExecution.getExitStatus();
    }
}



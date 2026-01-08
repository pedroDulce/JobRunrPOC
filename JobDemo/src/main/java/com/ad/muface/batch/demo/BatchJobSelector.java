package com.ad.muface.batch.demo;

import com.ad.muface.batch.notifier.BatchDispatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class BatchJobSelector extends BatchDispatcher {

    /**
     * Debe coincidir el nombre de la variable job con el método @Bean en la clase SpringBatchJob:
     * @Bean
     *     public Job dailyTransactionBatchJob(...)
     */
    private final Job dailyTransactionBatchJob;

    protected Job getJobToExecute() {
            return this.dailyTransactionBatchJob;
    }


}

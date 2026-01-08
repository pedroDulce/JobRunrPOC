package com.ad.muface.batch.dispatcher;

import com.ad.muface.batch.notifier.BatchDispatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class BatchInitizalizer extends BatchDispatcher {

    private final Job dailyTransactionBatchJob;

    protected Job getJobToExecute() {
            return this.dailyTransactionBatchJob;
    }


}

package org.example.batch.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class ExecutorConfig {

    @Bean("longRunningJobExecutor")
    public ExecutorService longRunningJobExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(20);
        executor.setThreadNamePrefix("long-job-");
        executor.setAllowCoreThreadTimeOut(true);
        executor.setKeepAliveSeconds(300);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();

        return executor.getThreadPoolExecutor();
    }

    @Bean("highPriorityExecutor")
    public ExecutorService highPriorityExecutor() {
        // Crear un ThreadFactory personalizado sin dependencias externas
        java.util.concurrent.ThreadFactory threadFactory = new java.util.concurrent.ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "high-priority-" + threadNumber.getAndIncrement());
                thread.setPriority(Thread.MAX_PRIORITY);
                thread.setDaemon(false);
                return thread;
            }
        };

        return java.util.concurrent.Executors.newFixedThreadPool(3, threadFactory);
    }

    @Bean("normalPriorityExecutor")
    public ExecutorService normalPriorityExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("normal-job-");
        executor.setAllowCoreThreadTimeOut(true);
        executor.setKeepAliveSeconds(60);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();

        return executor.getThreadPoolExecutor();
    }
}

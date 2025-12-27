package com.company.batchscheduler;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
@OpenAPIDefinition(
        info = @Info(
                title = "Batch Scheduler API",
                version = "1.0",
                description = "API para programación y gestión de jobs batch"
        )
)
public class SchedulerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerServiceApplication.class, args);
        System.out.println("\n=========================================");
        System.out.println("🚀 Batch Scheduler Service INICIADO");
        System.out.println("=========================================");
        System.out.println("📊 Dashboard: http://localhost:8000");
        System.out.println("🔗 API Docs: http://localhost:8080/swagger-ui.html");
        System.out.println("🏥 Health: http://localhost:8080/api/v1/health");
        System.out.println("=========================================\n");
    }
}

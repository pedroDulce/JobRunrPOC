package com.company.batchscheduler.model.negocio;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "customer_transactions")
@Data
public class CustomerTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(columnDefinition = "BIGSERIAL")  // <-- Especificar tipo explícito
    private Long id;

    @Column(name = "transaction_id", unique = true, length = 50)
    private String transactionId;

    // ... resto de campos con @Column apropiados
    @Column(name = "customer_id", length = 50)
    private String customerId;

    @Column(precision = 10, scale = 2)
    private BigDecimal amount;

    @Column(length = 3)
    private String currency;

    @Column(name = "transaction_date")
    private LocalDate transactionDate;

    @Column(length = 20)
    private String status;

    @Column(name = "source_file", length = 255)
    private String sourceFile;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
}

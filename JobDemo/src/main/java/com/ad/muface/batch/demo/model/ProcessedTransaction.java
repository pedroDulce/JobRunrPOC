package com.ad.muface.batch.demo.model;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDate;

@Data
public class ProcessedTransaction {
    private Long id;
    private String transactionId;
    private String customerId;
    private BigDecimal amount;
    private String currency;
    private LocalDate transactionDate;
    private String status; // PROCESSED, ERROR, etc.
    private Integer partitionNumber;
    private String processingNotes;
    private LocalDate processedDate = LocalDate.now();

    // Métodos auxiliares
    public boolean isSuccessfullyProcessed() {
        return "PROCESSED".equals(status);
    }
}

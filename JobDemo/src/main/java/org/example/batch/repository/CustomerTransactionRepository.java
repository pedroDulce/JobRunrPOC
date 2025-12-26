package org.example.batch.repository;

import org.example.batch.model.CustomerTransaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface CustomerTransactionRepository extends JpaRepository<CustomerTransaction, Long> {

    List<CustomerTransaction> findByStatusAndTransactionDate(
            String status, LocalDate transactionDate);

    List<CustomerTransaction> findByStatus(String status);

    @Query("SELECT ct FROM CustomerTransaction ct " +
            "WHERE ct.status = 'ENQUEUED' " +
            "AND ct.transactionDate <= :date " +
            "ORDER BY ct.transactionDate, ct.customerId")
    List<CustomerTransaction> findPendingUntilDate(@Param("date") LocalDate date);

    @Modifying
    @Transactional
    @Query("UPDATE CustomerTransaction ct SET ct.status = 'PROCESSED' " +
            "WHERE ct.id IN :ids")
    void markAsProcessed(@Param("ids") List<Long> ids);

}


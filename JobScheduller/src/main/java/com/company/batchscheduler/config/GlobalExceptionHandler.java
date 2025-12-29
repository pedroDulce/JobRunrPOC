package com.company.batchscheduler.config;

import lombok.extern.slf4j.Slf4j;
import org.jobrunr.storage.sql.common.db.ConcurrentSqlModificationException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {


    /**
     * Captura otra excepción específica con logueo controlado
     */
    @ExceptionHandler(org.jobrunr.storage.ConcurrentJobModificationException.class)
    public void handleConcurrentJobModifyExc(org.jobrunr.storage.ConcurrentJobModificationException ex) {

        // Loguear solo a un archivo específico, no a consola
        log.warn("Error controlado por ConcurrentJobModificationException");

    }

    @ExceptionHandler(ConcurrentSqlModificationException.class)
    public void handleConcurrentModifyExc2(ConcurrentSqlModificationException ex) {

        // Loguear solo a un archivo específico, no a consola
        log.warn("Error controlado por ConcurrentSqlModificationException");

    }



}


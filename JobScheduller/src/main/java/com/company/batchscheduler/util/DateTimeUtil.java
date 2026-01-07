package com.company.batchscheduler.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateTimeUtil {

    private static final DateTimeFormatter DD_MM_YYYY_HH_MM_SS =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    private static final DateTimeFormatter DD_MM_YYYY =
            DateTimeFormatter.ofPattern("dd/MM/yyyy");

    private static final DateTimeFormatter HH_MM_SS =
            DateTimeFormatter.ofPattern("HH:mm:ss");

    // Para el momento actual
    public static String ahora() {
        return DD_MM_YYYY_HH_MM_SS.format(Instant.now());
    }

    // Solo fecha
    public static String fechaActual() {
        return DD_MM_YYYY.format(Instant.now());
    }

    // Solo hora
    public static String horaActual() {
        return HH_MM_SS.format(Instant.now());
    }

    // Formatear con formato personalizado
    public static String formatear(Instant dateTime, String formato) {
        return DateTimeFormatter.ofPattern(formato).format(dateTime);
    }

    // Formatear con el formato estándar
    public static String formatear(Instant dateTime) {
        return DD_MM_YYYY_HH_MM_SS.format(dateTime);
    }

    // Validar y formatear
    public static String formatearSeguro(LocalDateTime dateTime) {
        if (dateTime == null) {
            return "Fecha no disponible";
        }
        return DD_MM_YYYY_HH_MM_SS.format(dateTime);
    }

    public static String formatNow() {
        return DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
                .withZone(ZoneId.systemDefault())
                .format(Instant.now());
    }

}

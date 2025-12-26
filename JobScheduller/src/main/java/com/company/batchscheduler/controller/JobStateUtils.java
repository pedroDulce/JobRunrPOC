package com.company.batchscheduler.controller;

import org.jobrunr.jobs.states.StateName;

public class JobStateUtils {

    public static boolean isTerminalState(StateName state) {
        return state == StateName.SUCCEEDED ||
                state == StateName.FAILED ||
                state == StateName.DELETED;
    }

    public static String getStateDescription(StateName state) {
        switch (state) {
            case SCHEDULED: return "Programado";
            case ENQUEUED: return "En cola";
            case PROCESSING: return "En proceso";
            case SUCCEEDED: return "Completado exitosamente";
            case FAILED: return "Fallado";
            case DELETED: return "Eliminado";
            default: return "Desconocido";
        }
    }
}


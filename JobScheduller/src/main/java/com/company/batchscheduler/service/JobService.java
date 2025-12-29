package com.company.batchscheduler.service;

import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

@Slf4j
@Service
public class JobService {
    private final JobRunrAdminRepository jobRunrAdminRepository;
    private final StorageProvider storageProvider;
    private Class<?> jobClass;
    private Map<String, Method> jobMethods = new HashMap<>();

    public JobService(StorageProvider storageProvider, JobRunrAdminRepository jobRunrAdminRepository) {
        this.storageProvider = storageProvider;
        this.jobRunrAdminRepository = jobRunrAdminRepository;
        initializeJobClass();
        cacheJobMethods();
    }

    /**
     * Inicializa la clase Job usando reflexión
     */
    private void initializeJobClass() {
        try {
            jobClass = Class.forName(Job.class.getName());
            log.error("Clase Job encontrada: ", jobClass.getName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("No se pudo encontrar la clase Job");
        }
    }

    /**
     * Imprime todos los metadatos del job para debug
     */
    private void printJobMetadata(Job job) {
        try {
            System.out.println("\n=== METADATOS DEL JOB ===");

            // Obtener metadatos
            Map<String, Object> metadata = job.getMetadata();

            if (metadata == null || metadata.isEmpty()) {
                System.out.println("No hay metadatos disponibles");
                return;
            }

            // Imprimir cada metadato
            System.out.println("Total de metadatos: " + metadata.size());
            System.out.println("--------------------------------");

            for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                System.out.printf("%-25s : ", key);

                if (value == null) {
                    System.out.println("null");
                } else {
                    System.out.println(value + " (" + value.getClass().getSimpleName() + ")");
                }
            }

            System.out.println("================================\n");

        } catch (Exception e) {
            System.err.println("Error imprimiendo metadatos: " + e.getMessage());
        }
    }


    /**
     * Método para imprimir TODA la información del job (no solo metadatos)
     */
    public void printCompleteJobInfo(UUID jobId) {
        try {
            Job job = getById(jobId);
            if (job == null) {
                log.info("Job no encontrado: " + jobId);
                return;
            }

            Class<?> jobClass = job.getClass();

            log.info("\n=== INFORMACIÓN COMPLETA DEL JOB ===");
            log.info("ID: " + jobId);
            log.info("Clase: " + jobClass.getName());

            // 1. Imprimir campos
            log.info("\n--- CAMPOS ---");
            for (java.lang.reflect.Field field : jobClass.getDeclaredFields()) {
                field.setAccessible(true);
                log.info("fieldname {}, fieldType {}", field.getName(), field.getType().getSimpleName());

                try {
                    Object value = field.get(job);
                    log.info(value != null ? (String) value : "null");
                } catch (Exception e) {
                    log.error("[Error accediendo]");
                }
            }

            // 2. Imprimir metadatos
            printJobMetadata(job);

            // 3. Imprimir métodos getter
            log.info("\n--- GETTERS DISPONIBLES ---");
            for (java.lang.reflect.Method method : jobClass.getMethods()) {
                if (method.getName().startsWith("get") &&
                        method.getParameterCount() == 0 &&
                        !method.getName().equals("getClass")) {
                    try {
                        Object value = method.invoke(job);
                        log.info("metodo: " + method.getName() + "()" + value != null ? value.toString() : "null");
                    } catch (Exception e) {
                        log.info("[Error]: " + method.getName() + "()" + method.getName() + "()", e.getCause() != null ?
                                e.getCause().getMessage() : e.getMessage());
                    }
                }
            }

            log.info("====================================\n");

        } catch (Exception e) {
            log.error("Error imprimiendo información del job: " + e.getMessage());
        }
    }

    /**
     * Actualizar metadata del job
     */
    public boolean updateMetadata(UUID jobId) {
        try {
            // 1. Obtener el job existente para copiar sus datos
            Job existingJob = getById(jobId);

            if (existingJob == null) {
                return false;
            }

            /*JobRunrMetadata metadata = new JobRunrMetadata();
            existingJob.getMetadata();

            // 4. Guardar el nuevo objeto
            storageProvider.saveMetadata();*/

            return true;

        } catch (Exception e) {
            log.error("Error: " + e.getMessage());
            return false;
        }
    }

    /**
     * Cachea todos los métodos de la clase Job para mejor performance
     */
    private void cacheJobMethods() {
        try {
            Method[] methods = jobClass.getMethods();
            for (Method method : methods) {
                jobMethods.put(method.getName(), method);
            }

            log.info("Métodos cacheados: {} ", jobMethods.size());

        } catch (Exception e) {
            throw new RuntimeException("Error cacheando métodos Job", e);
        }
    }

    /**
     * Método principal para actualizar estado y fecha de finalización
     * newStatus permitidos:
     * ENQUEUED
     * SCHEDULED
     * PROCESSING
     * SUCCEEDED
     * FAILED
     * DELETED
     */
    public boolean updateJobStatus(UUID jobId, String newStatus, Date completionDate) {
        try {
            log.info("Actualizando job " + jobId + " a estado: " + newStatus);

            // 1. Obtener el job existente
            Job job = getById(jobId);

            if (job == null) {
                log.error("Job no encontrado: " + jobId);
                return false;
            }

            jobRunrAdminRepository.updateJobState(job.getId(), newStatus);

            log.info("Job actualizado exitosamente");
            return true;

        } catch (Exception e) {
            log.error("Error actualizando job " + jobId + ": " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Invoca setStatus() en el objeto Job
     */
    private void invokeSetStatus(Object job, String status) throws Exception {
        invokeSetterMethod(job, "status", status, String.class);
    }

    /**
     * Invoca setCompletionDate() en el objeto Job
     */
    private void invokeSetCompletionDate(Object job, Date completionDate) throws Exception {
        invokeSetterMethod(job, "completionDate", completionDate, Date.class);
    }

    /**
     * Invoca setLastModified() en el objeto Job
     */
    private void invokeSetLastModified(Object job, Date lastModified) throws Exception {
        invokeSetterMethod(job, "lastModified", lastModified, Date.class);
    }

    /**
     * Método genérico para invocar setters
     */
    private void invokeSetterMethod(Object job, String fieldName, Object value, Class<?> paramType)
            throws Exception {

        String setterName = "set" + capitalize(fieldName);

        // Buscar en cache primero
        Method setter = jobMethods.get(setterName);

        if (setter == null) {
            // Intentar con el tipo de parámetro específico
            try {
                setter = jobClass.getMethod(setterName, paramType);
                jobMethods.put(setterName, setter); // Cachear
            } catch (NoSuchMethodException e) {
                // Intentar con tipos alternativos
                setter = findSetterWithAlternativeTypes(job, setterName, value, paramType);
            }
        }

        if (setter != null) {
            setter.invoke(job, value);
        } else {
            throw new NoSuchMethodException("Setter no encontrado: " + setterName +
                    "(" + paramType.getSimpleName() + ")");
        }
    }

    /**
     * Busca setter con tipos alternativos
     */
    private Method findSetterWithAlternativeTypes(Object job, String setterName,
                                                  Object value, Class<?> preferredType)
            throws Exception {

        Class<?>[] alternativeTypes = getAlternativeTypes(preferredType);

        for (Class<?> altType : alternativeTypes) {
            try {
                Method method = jobClass.getMethod(setterName, altType);

                // Convertir valor al tipo alternativo si es necesario
                Object convertedValue = convertValue(value, altType);
                method.invoke(job, convertedValue);

                jobMethods.put(setterName, method); // Cachear
                return method;

            } catch (NoSuchMethodException | IllegalArgumentException e) {
                // Continuar con el siguiente tipo
            }
        }

        return null;
    }

    /**
     * Obtiene tipos alternativos para conversión
     */
    private Class<?>[] getAlternativeTypes(Class<?> originalType) {
        if (originalType == String.class) {
            return new Class<?>[]{Object.class, CharSequence.class};
        } else if (originalType == Date.class) {
            return new Class<?>[]{Long.class, java.sql.Date.class, java.sql.Timestamp.class};
        } else if (originalType == Integer.class) {
            return new Class<?>[]{int.class, Long.class, long.class, Number.class};
        }
        return new Class<?>[]{Object.class};
    }

    /**
     * Convierte valor a tipo alternativo
     */
    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null) return null;

        if (targetType.isAssignableFrom(value.getClass())) {
            return value;
        }

        // Conversiones específicas
        if (value instanceof Date && targetType == Long.class) {
            return ((Date) value).getTime();
        } else if (value instanceof Date && targetType == java.sql.Date.class) {
            return new java.sql.Date(((Date) value).getTime());
        } else if (value instanceof Date && targetType == java.sql.Timestamp.class) {
            return new java.sql.Timestamp(((Date) value).getTime());
        } else if (value instanceof Integer && targetType == Long.class) {
            return ((Integer) value).longValue();
        }

        return value; // Dejar que falle si no se puede convertir
    }

    /**
     * Método alternativo para buscar job
     */
    private Object findJobAlternative(String jobId) {
        try {
            // Intentar con un método más genérico
            Method method = storageProvider.getClass()
                    .getMethod("execute", Map.class);

            Map<String, Object> query = new HashMap<>();
            query.put("operationType", "find");
            query.put("jobId", jobId);
            query.put("entityType", "Job");

            Object result = method.invoke(storageProvider, query);

            if (result instanceof Map) {
                return convertMapToJob((Map<?, ?>) result);
            }

            return result;

        } catch (Exception e) {
            System.err.println("Error en búsqueda alternativa: " + e.getMessage());
            return null;
        }
    }

    /**
     * Convierte Map a objeto Job
     */
    private Object convertMapToJob(Map<?, ?> data) throws Exception {
        Object job = jobClass.newInstance();

        for (Map.Entry<?, ?> entry : data.entrySet()) {
            String fieldName = entry.getKey().toString();
            Object value = entry.getValue();

            if (value != null) {
                try {
                    invokeSetterMethod(job, fieldName, value, value.getClass());
                } catch (Exception e) {
                    // Ignorar campos que no se puedan setear
                }
            }
        }

        return job;
    }

    /**
     * Métodos de conveniencia
     */
    public boolean completeSuccessJob(UUID jobId) {
        return updateJobStatus(jobId, StateName.SUCCEEDED.toString(), new Date());
    }

    public boolean failJob(UUID jobId, String errorMessage) {
        try {
            Job job = getById(jobId);
            if (job == null) {
                log.info("Job no encontrado: " + jobId);
                return false;
            }

            invokeSetStatus(job, StateName.FAILED.toString());
            invokeSetCompletionDate(job, new Date());
            invokeSetLastModified(job, new Date());

            // Intentar setear mensaje de error si existe el setter
            try {
                invokeSetterMethod(job, "errorMessage", errorMessage, String.class);
            } catch (Exception e) {
                // Ignorar si no existe setErrorMessage
            }

            storageProvider.save(job);
            return true;

        } catch (Exception e) {
            System.err.println("Error marcando job como fallido: " + e.getMessage());
            return false;
        }
    }

    public boolean startJob(UUID jobId) {
        try {
            Job job = getById(jobId);
            if (job == null) return false;

            invokeSetStatus(job, StateName.PROCESSING.toString());
            invokeSetLastModified(job, new Date());

            // Intentar setear fecha de inicio si existe
            try {
                invokeSetterMethod(job, "startDate", new Date(), Date.class);
            } catch (Exception e) {
                // Ignorar si no existe
            }

            storageProvider.save(job);
            return true;

        } catch (Exception e) {
            System.err.println("Error iniciando job: " + e.getMessage());
            return false;
        }
    }

    public boolean continueJob(UUID jobId) {
        try {
            Job job = getById(jobId);
            if (job == null) return false;

            invokeSetStatus(job, StateName.PROCESSING.toString());
            invokeSetLastModified(job, new Date());

            storageProvider.save(job);
            return true;

        } catch (Exception e) {
            System.err.println("Error iniciando job: " + e.getMessage());
            return false;
        }
    }

    public boolean pauseJob(UUID jobId) {
        return updateJobStatus(jobId, StateName.AWAITING.toString(), null);
    }

    public boolean cancelJob(UUID jobId) {
        return updateJobStatus(jobId, StateName.DELETED.toString(), new Date());
    }

    private Object createUpdatedJob(Object id, Object name, Object createdDate,
                                    String status, Date completionDate, Date lastModified)
            throws Exception {

        // Intentar diferentes constructores
        try {
            // Constructor con todos los parámetros
            Constructor<?> constructor = Job.class.getConstructor(
                    String.class, // id
                    String.class, // name
                    Date.class,   // createdDate
                    String.class, // status
                    Date.class,   // completionDate
                    Date.class    // lastModified
            );

            return constructor.newInstance(id, name, createdDate, status, completionDate, lastModified);

        } catch (NoSuchMethodException e) {
            // Constructor con menos parámetros
            try {
                Constructor<?> constructor = Job.class.getConstructor(
                        String.class, // id
                        String.class, // status
                        Date.class    // completionDate
                );

                return constructor.newInstance(id, status, completionDate);

            } catch (NoSuchMethodException e2) {
                // Usar constructor por defecto y setters
                Object job = Job.class.newInstance();
                setFieldViaReflection(job, "jobId", id);
                setFieldViaReflection(job, "status", status);
                setFieldViaReflection(job, "completionDate", completionDate);
                setFieldViaReflection(job, "lastModified", lastModified);
                return job;
            }
        }
    }

    private void setFieldViaReflection(Object obj, String fieldName, Object value)
            throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    private Object getFieldValue(Object obj, String fieldName) throws Exception {
        try {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(obj);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("No se puede obtener campo " + fieldName, e);
        }
    }

    public Job getById(UUID jobId) {
        return storageProvider.getJobById(jobId);
    }


    /**
     * Eliminar un job por su ID
     */
    public boolean deleteJob(String jobId) {
        try {
            storageProvider.deleteRecurringJob(jobId);
            log.info("Job {} deleted successfully", jobId);
            return true;
        } catch (Exception e) {
            log.error("Error deleting job {}: {}", jobId, e.getMessage());
            return false;
        }
    }

    /**
     * Obtener información de un job (VERSIÓN CORREGIDA)
     */
    public Map<String, Object> getJobInfo(String jobId) {
        try {
            Job job = storageProvider.getJobById(UUID.fromString(jobId));
            if (job == null) {
                return null;
            }

            JobDetails jobDetails = job.getJobDetails();

            Map<String, Object> jobInfo = new HashMap<>();
            jobInfo.put("id", job.getId().toString());
            jobInfo.put("jobName", jobDetails.getClassName() + "." + jobDetails.getMethodName());
            jobInfo.put("state", job.getState().name());
            jobInfo.put("createdAt", job.getCreatedAt());
            jobInfo.put("updatedAt", job.getUpdatedAt());
            jobInfo.put("jobSignature", jobDetails.getClassName());

            // Obtener scheduledAt si está programado
            if (job.hasState(StateName.AWAITING)) {
                ScheduledState scheduledState = job.getJobState();
                jobInfo.put("scheduledAt", scheduledState.getScheduledAt());
            }

            // Información adicional del job
            jobInfo.put("className", jobDetails.getClassName());
            jobInfo.put("methodName", jobDetails.getMethodName());
            jobInfo.put("jobParameters", jobDetails.getJobParameters());
            //jobInfo.put("labels", jobDetails.getLabels());

            return jobInfo;
        } catch (Exception e) {
            log.error("Error getting job info {}: {}", jobId, e.getMessage());
            return null;
        }
    }

    /**
     * Helper para convertir Job a Map
     */
    private Map<String, Object> convertJobToMap(Job job) {
        Map<String, Object> jobMap = new HashMap<>();
        JobDetails details = job.getJobDetails();

        jobMap.put("id", job.getId().toString());
        jobMap.put("jobName", details.getClassName());
        jobMap.put("className", details.getClassName());
        jobMap.put("methodName", details.getMethodName());
        jobMap.put("state", job.getState().name());
        jobMap.put("createdAt", job.getCreatedAt());
        jobMap.put("updatedAt", job.getUpdatedAt());
        //jobMap.put("labels", details.getLabels());

        // Información específica por estado
        switch (job.getState()) {
            case SCHEDULED:
                jobMap.put("scheduledAt", ((ScheduledState) job.getJobState()).getScheduledAt());
                break;
            case PROCESSING:
                jobMap.put("startedAt", job.getUpdatedAt());
                break;
            case SUCCEEDED:
                jobMap.put("completedAt", job.getUpdatedAt());
                break;
            case FAILED:
                jobMap.put("failedAt", job.getUpdatedAt());
                jobMap.put("errorMessage", job.getJobState().getName());
                break;
        }

        return jobMap;
    }


    /**
     * Eliminar un job recurrente por nombre
     */
    public boolean deleteRecurringJobByName(String jobName) {
        try {
            storageProvider.deleteRecurringJob(jobName);
            log.info("Recurring job {} deleted", jobName);
            return true;
        } catch (Exception e) {
            log.error("Error deleting recurring job {}: {}", jobName, e.getMessage());
            return false;
        }
    }


    /**
     * Capitaliza la primera letra de un string
     */
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) return str;
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }


}

package org.example.batch;

import lombok.Data;


/** TODO: meter esta clase en la infrabackend **/
@Data
public class JobResult {

    private boolean success;

    public static JobResult success(Object result) {
        JobResult res = new JobResult();
        res.setSuccess(true);
        return res;
    }

    public static JobResult failure(String message) {
        JobResult res = new JobResult();
        res.setSuccess(false);
        return res;
    }

    public String getMessage() {
        return success ? "Proceso finalizado de forma exitosa" : "Proceso finalizado con errores";
    }

}


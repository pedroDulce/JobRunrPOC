package common.batch.dto;

import lombok.Data;

/** TODO: meter esta clase en la infrabackend **/
@Data
public class JobResult {

    private boolean success;
    private Object result;
    private String message;

    public static JobResult success(Object result) {
        JobResult res = new JobResult();
        res.setSuccess(true);
        res.setResult(result);
        res.setMessage("Proceso finalizado de forma exitosa");
        return res;
    }

    public static JobResult failure(String message) {
        JobResult res = new JobResult();
        res.setSuccess(false);
        res.setMessage(message);
        return res;
    }

    public String getMessage() {
        return success ? this.message : "Proceso finalizado con errores, motivo: " + this.message;
    }
}


package common.batch.dto;

import lombok.Data;

import java.time.LocalDate;

@Data
public class ImmediateJobRequest {
    private LocalDate processDate;
    private boolean sendEmail;
    private String emailRecipient;
}

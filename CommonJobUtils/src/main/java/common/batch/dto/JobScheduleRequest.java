package common.batch.dto;

import lombok.Data;

import java.time.LocalDate;

@Data
public class JobScheduleRequest {
    private String jobName;
    private String cronExpression;
    private LocalDate processDate;
    private boolean sendEmail;
    private String emailRecipient;
    private String customerFilter;
}

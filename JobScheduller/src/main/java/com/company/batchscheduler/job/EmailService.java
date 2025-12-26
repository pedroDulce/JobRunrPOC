package com.company.batchscheduler.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EmailService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailService.class);

    public void sendEmail(String recipient, String subject, String body) {
        LOGGER.info("sending mail... to " + subject + " with body content: " + body);
    }

}

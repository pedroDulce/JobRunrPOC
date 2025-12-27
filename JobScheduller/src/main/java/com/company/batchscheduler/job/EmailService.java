package com.company.batchscheduler.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class EmailService {

    public void sendEmail(String recipient, String subject, String body) {
        log.info("sending mail... to " + subject + " with body content: " + body);
    }

}

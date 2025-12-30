package com.company.batchscheduler.controller;

import com.company.batchscheduler.service.JobManagementOperations;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/health")
public class HealthController {

    private final ApplicationEventPublisher eventPublisher;

    private final JobManagementOperations jobManagementOperations;

    @GetMapping
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("timestamp", System.currentTimeMillis());
        health.put("service", "Batch Scheduler Service");
        health.put("version", "1.0.0");

        return ResponseEntity.ok(health);
    }

    @PostMapping("/liveness/{state}")
    public ResponseEntity<String> setLiveness(@PathVariable String state) {
        switch (state.toUpperCase()) {
            case "CORRECT":
                AvailabilityChangeEvent.publish(eventPublisher, this, LivenessState.CORRECT);
                return ResponseEntity.ok("Liveness set to CORRECT");
            case "BROKEN":
                AvailabilityChangeEvent.publish(eventPublisher, this, LivenessState.BROKEN);
                return ResponseEntity.ok("Liveness set to BROKEN");
            default:
                return ResponseEntity.badRequest().body("Invalid state");
        }
    }
}

package com.ad.muface.jobs.infra;

import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

@Component
public class InstanceIdentifier {
    private String instanceId;

    public InstanceIdentifier() {
        // Usar hostname de Kubernetes + timestamp
        String hostname = System.getenv("HOSTNAME");
        if (hostname == null || hostname.isEmpty()) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException exc) {
                this.instanceId = hostname + "-" + UUID.randomUUID().toString().substring(0, 8);
            }
        }
        this.instanceId = hostname + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String getInstanceId() {
        return instanceId;
    }
}


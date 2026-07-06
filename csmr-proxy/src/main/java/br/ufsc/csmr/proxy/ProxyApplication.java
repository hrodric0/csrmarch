package br.ufsc.csmr.proxy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * CSMR Proxy – Spring Boot entry point.
 *
 * This module implements the "Proxy" layer of the CSMR architecture (Figure 26, Alves 2026):
 *
 *   Client → [API] → [ReplicaMapper] → [MulticastService (URingPaxos)] → [KVS/Log replicas]
 *                                                ↓ collectible outputs D = {w1,...,wn}
 *                        Client ← [OutputProcessing f(D)] ←─────────────────────────────
 *
 * The key design constraint (Bonatto 2026):
 *   Because URingPaxos requires each proposer to propose to a single ring,
 *   the ReplicaMapper DUPLICATES client requests, generating independent
 *   calls for the KVS ring AND the Log ring simultaneously.
 */
@SpringBootApplication
public class ProxyApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProxyApplication.class, args);
    }
}





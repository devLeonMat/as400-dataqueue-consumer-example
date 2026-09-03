package com.example.as400.infrastructure.adapter.out.idempotency;

import com.example.as400.application.port.out.IdempotencyPort;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

@Component
public final class InMemoryIdempotencyAdapter implements IdempotencyPort {
    private static final Duration RETENTION = Duration.ofHours(24);
    private final ConcurrentHashMap<String, Instant> claims = new ConcurrentHashMap<>();
    private final AtomicLong operations = new AtomicLong();

    @Override
    public boolean claim(String messageId) {
        if ((operations.incrementAndGet() & 1023) == 0) {
            purgeExpired();
        }
        return claims.putIfAbsent(messageId, Instant.now()) == null;
    }

    @Override
    public void release(String messageId) {
        claims.remove(messageId);
    }

    private void purgeExpired() {
        Instant threshold = Instant.now().minus(RETENTION);
        claims.entrySet().removeIf(entry -> entry.getValue().isBefore(threshold));
    }
}

package com.example.as400.domain;

import java.time.Instant;
import java.util.Objects;

public record DataQueueMessage(String id, String correlationId, String payload, Instant receivedAt) {

    public DataQueueMessage {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(correlationId, "correlationId");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(receivedAt, "receivedAt");
    }
}

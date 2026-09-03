package com.example.as400.infrastructure.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("consumer")
public record ConsumerProperties(
        int bufferCapacity,
        int concurrency,
        Duration readTimeout,
        Duration shutdownTimeout,
        Backoff backoff,
        As400 as400,
        Fake fake,
        ExternalApi externalApi,
        Kafka kafka) {

    public ConsumerProperties {
        if (bufferCapacity < 1 || concurrency < 1) {
            throw new IllegalArgumentException("bufferCapacity and concurrency must be positive");
        }
    }

    public record Backoff(Duration initial, Duration maximum, double multiplier, double jitter) {}

    public record As400(String host, String username, String password, String queuePath) {}

    public record Fake(long messageCount, Duration interval) {}

    public record ExternalApi(String baseUrl, String path, boolean enabled) {}

    public record Kafka(String topic) {}
}

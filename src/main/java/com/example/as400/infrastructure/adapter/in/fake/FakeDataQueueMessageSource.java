package com.example.as400.infrastructure.adapter.in.fake;

import com.example.as400.application.port.out.DataQueueMessageSource;
import com.example.as400.domain.DataQueueMessage;
import com.example.as400.infrastructure.config.ConsumerProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"local", "test"})
public final class FakeDataQueueMessageSource implements DataQueueMessageSource {
    private final long messageCount;
    private final Duration interval;
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicBoolean connected = new AtomicBoolean();

    @Autowired
    public FakeDataQueueMessageSource(ConsumerProperties properties) {
        this(properties.fake().messageCount(), properties.fake().interval());
    }

    public FakeDataQueueMessageSource(long messageCount, Duration interval) {
        this.messageCount = messageCount;
        this.interval = interval;
    }

    @Override
    public void connect() {
        connected.set(true);
    }

    @Override
    public Optional<DataQueueMessage> readBlocking(Duration timeout) throws InterruptedException {
        if (!connected.get()) {
            throw new IllegalStateException("Fake queue is not connected");
        }
        long current = sequence.getAndIncrement();
        if (current >= messageCount) {
            Thread.sleep(Math.min(timeout.toMillis(), 25));
            return Optional.empty();
        }
        if (!interval.isZero()) {
            Thread.sleep(interval);
        }
        String id = "fake-%05d".formatted(current);
        String payload = "{\"id\":\"%s\",\"sequence\":%d}".formatted(id, current);
        return Optional.of(new DataQueueMessage(id, "corr-" + id, payload, Instant.now()));
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public void close() {
        connected.set(false);
    }

    public long generatedCount() {
        return Math.min(sequence.get(), messageCount);
    }
}

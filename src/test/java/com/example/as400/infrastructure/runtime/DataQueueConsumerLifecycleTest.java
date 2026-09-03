package com.example.as400.infrastructure.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.as400.application.port.in.ProcessMessageUseCase;
import com.example.as400.infrastructure.adapter.in.fake.FakeDataQueueMessageSource;
import com.example.as400.infrastructure.config.ConsumerProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class DataQueueConsumerLifecycleTest {

    @Test
    void drainsBoundedBufferWithConfiguredConcurrency() {
        int count = 1_000;
        var source = new FakeDataQueueMessageSource(count, Duration.ZERO);
        var processed = new AtomicInteger();
        ProcessMessageUseCase processor = message -> Mono.fromRunnable(processed::incrementAndGet);
        var properties = properties(8, 4);
        var lifecycle = new DataQueueConsumerLifecycle(source, processor, properties, new SimpleMeterRegistry());

        lifecycle.start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(processed).hasValue(count));
        lifecycle.stop();

        assertThat(lifecycle.isRunning()).isFalse();
        assertThat(lifecycle.bufferSize()).isZero();
    }

    private static ConsumerProperties properties(int bufferCapacity, int concurrency) {
        return new ConsumerProperties(
                bufferCapacity,
                concurrency,
                Duration.ofMillis(10),
                Duration.ofSeconds(5),
                new ConsumerProperties.Backoff(Duration.ofMillis(1), Duration.ofMillis(10), 2, 0),
                new ConsumerProperties.As400("unused", "unused", "unused", "/unused"),
                new ConsumerProperties.Fake(1_000, Duration.ZERO),
                new ConsumerProperties.ExternalApi("http://localhost", "/messages", false),
                new ConsumerProperties.Kafka("test"));
    }
}

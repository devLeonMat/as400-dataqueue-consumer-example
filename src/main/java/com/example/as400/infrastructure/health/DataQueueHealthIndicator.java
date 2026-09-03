package com.example.as400.infrastructure.health;

import com.example.as400.infrastructure.runtime.DataQueueConsumerLifecycle;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("as400DataQueue")
public final class DataQueueHealthIndicator implements HealthIndicator {
    private final DataQueueConsumerLifecycle lifecycle;

    public DataQueueHealthIndicator(DataQueueConsumerLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public Health health() {
        var details = Health.status(lifecycle.isConnected() ? "UP" : "OUT_OF_SERVICE")
                .withDetail("running", lifecycle.isRunning())
                .withDetail("bufferSize", lifecycle.bufferSize())
                .withDetail("inFlight", lifecycle.inFlightCount());
        return details.build();
    }
}

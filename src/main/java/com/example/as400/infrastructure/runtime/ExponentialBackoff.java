package com.example.as400.infrastructure.runtime;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public final class ExponentialBackoff {
    private final Duration initial;
    private final Duration maximum;
    private final double multiplier;
    private final double jitter;
    private int attempt;

    public ExponentialBackoff(Duration initial, Duration maximum, double multiplier, double jitter) {
        if (initial.isNegative() || initial.isZero() || maximum.compareTo(initial) < 0) {
            throw new IllegalArgumentException("Invalid backoff range");
        }
        if (multiplier < 1 || jitter < 0 || jitter > 1) {
            throw new IllegalArgumentException("Invalid backoff multiplier or jitter");
        }
        this.initial = initial;
        this.maximum = maximum;
        this.multiplier = multiplier;
        this.jitter = jitter;
    }

    public Duration nextDelay() {
        double exponential = initial.toMillis() * Math.pow(multiplier, attempt++);
        long capped = Math.min(maximum.toMillis(), Math.round(exponential));
        double factor = jitter == 0 ? 1 : 1 + ThreadLocalRandom.current().nextDouble(-jitter, jitter);
        return Duration.ofMillis(Math.max(1, Math.min(maximum.toMillis(), Math.round(capped * factor))));
    }

    public void reset() {
        attempt = 0;
    }
}

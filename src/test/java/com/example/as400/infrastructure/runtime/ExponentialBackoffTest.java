package com.example.as400.infrastructure.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ExponentialBackoffTest {

    @Test
    void growsExponentiallyAndCapsAtMaximum() {
        var backoff = new ExponentialBackoff(Duration.ofMillis(10), Duration.ofMillis(80), 2, 0);

        assertThat(backoff.nextDelay()).isEqualTo(Duration.ofMillis(10));
        assertThat(backoff.nextDelay()).isEqualTo(Duration.ofMillis(20));
        assertThat(backoff.nextDelay()).isEqualTo(Duration.ofMillis(40));
        assertThat(backoff.nextDelay()).isEqualTo(Duration.ofMillis(80));
        assertThat(backoff.nextDelay()).isEqualTo(Duration.ofMillis(80));
    }

    @Test
    void resetStartsSequenceAgain() {
        var backoff = new ExponentialBackoff(Duration.ofMillis(10), Duration.ofSeconds(1), 2, 0);
        backoff.nextDelay();
        backoff.nextDelay();

        backoff.reset();

        assertThat(backoff.nextDelay()).isEqualTo(Duration.ofMillis(10));
    }
}

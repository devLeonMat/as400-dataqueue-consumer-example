package com.example.as400.infrastructure.adapter.in.fake;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class FakeDataQueueMessageSourceTest {

    @Test
    void simulatesFiftyThousandUniqueMessages() throws Exception {
        var source = new FakeDataQueueMessageSource(50_000, Duration.ZERO);
        var ids = new HashSet<String>(50_000);
        source.connect();

        for (int i = 0; i < 50_000; i++) {
            ids.add(source.readBlocking(Duration.ofMillis(1)).orElseThrow().id());
        }

        assertThat(ids).hasSize(50_000);
        assertThat(source.generatedCount()).isEqualTo(50_000);
        assertThat(source.readBlocking(Duration.ofMillis(1))).isEmpty();
    }
}

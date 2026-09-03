package com.example.as400.application.service;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.as400.application.port.out.DurableMessagePort;
import com.example.as400.application.port.out.ExternalApiPort;
import com.example.as400.application.port.out.IdempotencyPort;
import com.example.as400.domain.DataQueueMessage;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MessageProcessingServiceTest {
    @Mock ExternalApiPort externalApi;
    @Mock DurableMessagePort durablePort;
    @Mock IdempotencyPort idempotency;

    @Test
    void callsOutputsOnceForAClaimedMessage() {
        var message = new DataQueueMessage("1", "correlation-1", "{}", Instant.now());
        when(idempotency.claim("1")).thenReturn(true);
        when(externalApi.send(message)).thenReturn(Mono.empty());
        when(durablePort.publish(message)).thenReturn(Mono.empty());
        var service = new MessageProcessingService(externalApi, durablePort, idempotency, new SimpleMeterRegistry());

        StepVerifier.create(service.process(message)).verifyComplete();

        verify(externalApi, times(1)).send(message);
        verify(durablePort, times(1)).publish(message);
    }

    @Test
    void skipsAlreadyClaimedMessage() {
        var message = new DataQueueMessage("duplicate", "correlation-2", "{}", Instant.now());
        when(idempotency.claim("duplicate")).thenReturn(false);
        var service = new MessageProcessingService(externalApi, durablePort, idempotency, new SimpleMeterRegistry());

        StepVerifier.create(service.process(message)).verifyComplete();

        verify(externalApi, times(0)).send(message);
        verify(durablePort, times(0)).publish(message);
    }

    @Test
    void releasesClaimWhenProcessingFails() {
        var message = new DataQueueMessage("retryable", "correlation-3", "{}", Instant.now());
        when(idempotency.claim("retryable")).thenReturn(true);
        when(externalApi.send(message)).thenReturn(Mono.error(new IllegalStateException("downstream unavailable")));
        var service = new MessageProcessingService(externalApi, durablePort, idempotency, new SimpleMeterRegistry());

        StepVerifier.create(service.process(message)).expectError(IllegalStateException.class).verify();

        verify(idempotency).release("retryable");
    }
}

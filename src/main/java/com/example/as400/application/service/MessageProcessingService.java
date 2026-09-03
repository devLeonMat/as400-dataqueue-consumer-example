package com.example.as400.application.service;

import com.example.as400.application.port.in.ProcessMessageUseCase;
import com.example.as400.application.port.out.DurableMessagePort;
import com.example.as400.application.port.out.ExternalApiPort;
import com.example.as400.application.port.out.IdempotencyPort;
import com.example.as400.domain.DataQueueMessage;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public final class MessageProcessingService implements ProcessMessageUseCase {
    private static final Logger log = LoggerFactory.getLogger(MessageProcessingService.class);
    public static final String CORRELATION_ID = "correlationId";

    private final ExternalApiPort externalApi;
    private final DurableMessagePort durableMessagePort;
    private final IdempotencyPort idempotency;
    private final Counter processed;
    private final Counter duplicates;
    private final Counter failures;

    public MessageProcessingService(
            ExternalApiPort externalApi,
            DurableMessagePort durableMessagePort,
            IdempotencyPort idempotency,
            MeterRegistry meterRegistry) {
        this.externalApi = externalApi;
        this.durableMessagePort = durableMessagePort;
        this.idempotency = idempotency;
        this.processed = meterRegistry.counter("as400.messages.processed");
        this.duplicates = meterRegistry.counter("as400.messages.duplicates");
        this.failures = meterRegistry.counter("as400.messages.failures");
    }

    @Override
    public Mono<Void> process(DataQueueMessage message) {
        return Mono.defer(() -> {
                    if (!idempotency.claim(message.id())) {
                        duplicates.increment();
                        log.debug("Duplicate message skipped id={} correlationId={}", message.id(), message.correlationId());
                        return Mono.empty();
                    }
                    return externalApi.send(message)
                            .then(Mono.defer(() -> durableMessagePort.publish(message)))
                            .doOnSuccess(ignored -> processed.increment())
                            .doOnError(error -> {
                                failures.increment();
                                idempotency.release(message.id());
                            });
                })
                .contextWrite(Context.of(CORRELATION_ID, message.correlationId()));
    }
}

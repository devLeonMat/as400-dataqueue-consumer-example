package com.example.as400.infrastructure.adapter.out.durable;

import com.example.as400.application.port.out.DurableMessagePort;
import com.example.as400.domain.DataQueueMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@Profile("!kafka")
public final class LoggingDurableMessageAdapter implements DurableMessagePort {
    private static final Logger log = LoggerFactory.getLogger(LoggingDurableMessageAdapter.class);

    @Override
    public Mono<Void> publish(DataQueueMessage message) {
        return Mono.fromRunnable(() -> log.debug(
                "Durable output disabled; message acknowledged id={} correlationId={}",
                message.id(), message.correlationId()));
    }
}

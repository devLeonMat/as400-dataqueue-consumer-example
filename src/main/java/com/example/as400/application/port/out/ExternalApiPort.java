package com.example.as400.application.port.out;

import com.example.as400.domain.DataQueueMessage;
import reactor.core.publisher.Mono;

public interface ExternalApiPort {
    Mono<Void> send(DataQueueMessage message);
}

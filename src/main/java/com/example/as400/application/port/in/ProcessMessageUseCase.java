package com.example.as400.application.port.in;

import com.example.as400.domain.DataQueueMessage;
import reactor.core.publisher.Mono;

public interface ProcessMessageUseCase {
    Mono<Void> process(DataQueueMessage message);
}

package com.example.as400.infrastructure.adapter.out.http;

import com.example.as400.application.port.out.ExternalApiPort;
import com.example.as400.application.service.MessageProcessingService;
import com.example.as400.domain.DataQueueMessage;
import com.example.as400.infrastructure.config.ConsumerProperties;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
public final class WebClientExternalApiAdapter implements ExternalApiPort {
    private final WebClient webClient;
    private final ConsumerProperties properties;

    public WebClientExternalApiAdapter(WebClient externalWebClient, ConsumerProperties properties) {
        this.webClient = externalWebClient;
        this.properties = properties;
    }

    @Override
    public Mono<Void> send(DataQueueMessage message) {
        if (!properties.externalApi().enabled()) {
            return Mono.empty();
        }
        return Mono.deferContextual(context -> webClient.post()
                .uri(properties.externalApi().path())
                .header("X-Correlation-ID", context.getOrDefault(
                        MessageProcessingService.CORRELATION_ID, message.correlationId()))
                .bodyValue(message.payload())
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> response.createException())
                .toBodilessEntity()
                .then());
    }
}

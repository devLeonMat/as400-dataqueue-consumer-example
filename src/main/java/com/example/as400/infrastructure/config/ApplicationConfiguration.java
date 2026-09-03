package com.example.as400.infrastructure.config;

import com.example.as400.application.port.in.ProcessMessageUseCase;
import com.example.as400.application.port.out.DurableMessagePort;
import com.example.as400.application.port.out.ExternalApiPort;
import com.example.as400.application.port.out.IdempotencyPort;
import com.example.as400.application.service.MessageProcessingService;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration(proxyBeanMethods = false)
public class ApplicationConfiguration {

    @Bean
    ProcessMessageUseCase processMessageUseCase(
            ExternalApiPort externalApi,
            DurableMessagePort durableMessagePort,
            IdempotencyPort idempotencyPort,
            MeterRegistry registry) {
        return new MessageProcessingService(externalApi, durableMessagePort, idempotencyPort, registry);
    }

    @Bean
    WebClient externalWebClient(WebClient.Builder builder, ConsumerProperties properties) {
        return builder.baseUrl(properties.externalApi().baseUrl()).build();
    }
}

package com.example.as400.infrastructure.adapter.out.durable;

import com.example.as400.application.port.out.DurableMessagePort;
import com.example.as400.domain.DataQueueMessage;
import com.example.as400.infrastructure.config.ConsumerProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@Profile("kafka")
public final class KafkaDurableMessageAdapter implements DurableMessagePort {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ConsumerProperties properties;

    public KafkaDurableMessageAdapter(KafkaTemplate<String, String> kafkaTemplate, ConsumerProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Override
    public Mono<Void> publish(DataQueueMessage message) {
        return Mono.fromFuture(kafkaTemplate.send(properties.kafka().topic(), message.id(), message.payload()))
                .then();
    }
}

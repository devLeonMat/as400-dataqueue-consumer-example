package com.example.as400.infrastructure.adapter.in.as400;

import com.example.as400.application.port.out.DataQueueMessageSource;
import com.example.as400.domain.DataQueueMessage;
import com.example.as400.infrastructure.config.ConsumerProperties;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.DataQueue;
import com.ibm.as400.access.DataQueueEntry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!local & !test")
public final class Jt400DataQueueMessageSource implements DataQueueMessageSource {
    private final ConsumerProperties properties;
    private AS400 system;
    private DataQueue queue;

    public Jt400DataQueueMessageSource(ConsumerProperties properties) {
        this.properties = properties;
    }

    @Override
    public synchronized void connect() throws Exception {
        close();
        var config = properties.as400();
        system = new AS400(config.host(), config.username(), config.password().toCharArray());
        system.setGuiAvailable(false);
        system.connectService(AS400.DATAQUEUE);
        queue = new DataQueue(system, config.queuePath());
    }

    @Override
    public Optional<DataQueueMessage> readBlocking(Duration timeout) throws Exception {
        var activeQueue = queue;
        if (activeQueue == null) {
            throw new IllegalStateException("AS400 Data Queue is not connected");
        }
        int waitSeconds = Math.max(1, Math.toIntExact(timeout.toSeconds()));
        DataQueueEntry entry = activeQueue.read(waitSeconds);
        if (entry == null) {
            return Optional.empty();
        }
        String payload = new String(entry.getData(), StandardCharsets.UTF_8);
        String messageId = sha256(payload);
        return Optional.of(new DataQueueMessage(messageId, UUID.randomUUID().toString(), payload, Instant.now()));
    }

    @Override
    public synchronized boolean isConnected() {
        return system != null && system.isConnected(AS400.DATAQUEUE);
    }

    @Override
    public synchronized void close() {
        if (system != null) {
            system.disconnectService(AS400.DATAQUEUE);
        }
        queue = null;
        system = null;
    }

    private static String sha256(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest);
    }
}

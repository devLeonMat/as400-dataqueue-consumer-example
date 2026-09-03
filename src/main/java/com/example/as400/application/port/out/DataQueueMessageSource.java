package com.example.as400.application.port.out;

import com.example.as400.domain.DataQueueMessage;
import java.time.Duration;
import java.util.Optional;

public interface DataQueueMessageSource extends AutoCloseable {
    void connect() throws Exception;

    Optional<DataQueueMessage> readBlocking(Duration timeout) throws Exception;

    boolean isConnected();

    @Override
    void close();
}

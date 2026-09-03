package com.example.as400.application.port.out;

public interface IdempotencyPort {
    boolean claim(String messageId);

    void release(String messageId);
}

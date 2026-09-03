package com.example.as400;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"consumer.fake.message-count=10", "consumer.external-api.enabled=false"})
@ActiveProfiles("test")
class DataQueueConsumerApplicationTest {

    @Test
    void applicationContextStartsWithFakeAdapter() {}
}

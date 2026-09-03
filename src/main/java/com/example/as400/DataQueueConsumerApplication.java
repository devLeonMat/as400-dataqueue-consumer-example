package com.example.as400;

import com.example.as400.infrastructure.config.ConsumerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ConsumerProperties.class)
public class DataQueueConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataQueueConsumerApplication.class, args);
    }
}

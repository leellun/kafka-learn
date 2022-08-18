package com.newland.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaspringbootApplication {
    @Bean
    public NewTopic initiaTopic() {
        return new NewTopic("testtopic", 1, (short) 1);
    }
    @Bean
    public NewTopic updateTopic() {
        return new NewTopic("testtopic", 1, (short) 1);
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaspringbootApplication.class, args);
    }
}

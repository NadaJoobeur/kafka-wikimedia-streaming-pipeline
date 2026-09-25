package com.nadajoobeur.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;


@Configuration
public class wikimediaTopicConfig {

    @Bean
    public NewTopic wikimediaStramTopic() {
        return TopicBuilder
                .name("wikimedia-stream")
                .build();
    }
}

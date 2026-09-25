package com.nadajoobeur.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;

import static java.lang.String.format;

@Slf4j
public class WikimediaConsumer {

    @KafkaListener(topics= "wikimedia-stream", groupId= "myGroup")
    public void consumeMsg(String msg) {
        log.info(format("Consuming the message from wikimedia-stream Topic:: %s", msg));

    }
}

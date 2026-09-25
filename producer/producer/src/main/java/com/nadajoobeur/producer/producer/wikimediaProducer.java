package com.nadajoobeur.producer.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static java.lang.String.format;

@Service
@RequiredArgsConstructor
@Slf4j
public class wikimediaProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String msg){
        //log.info(format("Sending message to NadaJbr Topic:: %s", msg));
        kafkaTemplate.send("wikimedia-stream", msg);
    }
}

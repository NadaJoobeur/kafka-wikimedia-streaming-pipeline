package com.nadajoobeur.kafka_demo.consumer;

import com.nadajoobeur.kafka_demo.payload.Student;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import static java.lang.String.format;

@Service
@Slf4j
public class KafkaConsumer {

    //@KafkaListener(topics = "NadaJbr", groupId = "myGroup")
    public void consumeMsg(String msg) {
        log.info(format("Consuming the message from NadaJbr Topic:: %s", msg));
    }

    @KafkaListener(topics = "NadaJbr", groupId = "myGroup")
    public void consumeJsonMsg(Student student) {
        log.info(format("Consuming the message from NadaJbr Topic:: %s", student.toString()));
    }

}

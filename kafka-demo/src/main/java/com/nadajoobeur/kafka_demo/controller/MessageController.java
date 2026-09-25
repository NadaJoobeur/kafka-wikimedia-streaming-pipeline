package com.nadajoobeur.kafka_demo.controller;


import com.nadajoobeur.kafka_demo.payload.Student;
import com.nadajoobeur.kafka_demo.producer.kafkaJsonProducer;
import com.nadajoobeur.kafka_demo.producer.kafkaProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/messages")
@RequiredArgsConstructor

public class MessageController {

    private final kafkaProducer kafkaProducer;
    private final kafkaJsonProducer kafkaJsonProducer;

    @PostMapping
    public ResponseEntity<String> sendMessage(@RequestBody String message){
        kafkaProducer.sendMessage(message);
        return ResponseEntity.ok("Message queued successfully");
    }

    @PostMapping("/json")
    public ResponseEntity<String> sendJsonMessage(@RequestBody Student message){
        kafkaJsonProducer.sendMessage(message);
        return ResponseEntity.ok("Message queued successfully");
    }
}

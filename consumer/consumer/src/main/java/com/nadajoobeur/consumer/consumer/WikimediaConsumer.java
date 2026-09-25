package com.nadajoobeur.consumer.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nadajoobeur.consumer.document.WikimediaEvent;
import com.nadajoobeur.consumer.repository.WikimediaEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

import static java.lang.String.format;

@Service
@RequiredArgsConstructor
@Slf4j
public class WikimediaConsumer {

    private final WikimediaEventRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "wikimedia-stream", groupId = "myGroup")
    public void consumeMsg(String msg) {
        log.info(format("Consuming the message from wikimedia-stream Topic:: %s", msg));

        try {
            JsonNode node = objectMapper.readTree(msg);

            WikimediaEvent event = new WikimediaEvent();
            event.setType(textOrNull(node, "type"));
            event.setTitle(textOrNull(node, "title"));
            event.setUser(textOrNull(node, "user"));
            event.setWiki(textOrNull(node, "wiki"));
            event.setComment(textOrNull(node, "comment"));
            event.setTimestamp(node.hasNonNull("timestamp") ? node.get("timestamp").asLong() : null);
            event.setRawPayload(msg);
            event.setReceivedAt(Instant.now());

            repository.save(event);
        } catch (Exception e) {
            log.error("Failed to parse or persist Wikimedia event", e);
        }
    }

    private String textOrNull(JsonNode node, String field) {
        return node.hasNonNull(field) ? node.get(field).asText() : null;
    }
}
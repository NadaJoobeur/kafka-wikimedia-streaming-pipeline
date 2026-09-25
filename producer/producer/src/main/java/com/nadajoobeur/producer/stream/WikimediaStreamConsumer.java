package com.nadajoobeur.producer.stream;

import com.nadajoobeur.producer.producer.wikimediaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
@Slf4j

public class WikimediaStreamConsumer {

    private final WebClient webClient;

    private final wikimediaProducer producer;

    public WikimediaStreamConsumer(WebClient.Builder webClientBuilder, wikimediaProducer producer){

        this.webClient = webClientBuilder
                .baseUrl("https://stream.wikimedia.org/v2")
                .build();
        this.producer= producer;
    }

    public void consumeStreamAndPublish(){
        webClient.get()
                .uri("/stream/recentchange")
                .retrieve()
                .bodyToFlux(String.class)
                .subscribe(producer::sendMessage);
    }


}

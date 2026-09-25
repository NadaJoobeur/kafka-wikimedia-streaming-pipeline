package com.nadajoobeur.consumer.document;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Document(collection = "wikimedia_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class WikimediaEvent {

    @Id
    private String id;

    private String type;
    private String title;
    private String user;
    private String wiki;
    private String comment;
    private Long timestamp;

    private String rawPayload;
    private Instant receivedAt;
}
# Kafka Wikimedia Streaming Pipeline

[![Java](https://img.shields.io/badge/Java-17-orange)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.1-brightgreen)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-black)](https://kafka.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-Persistence-47A248)](https://www.mongodb.com/)

A hands-on Apache Kafka project built in two layers: a set of **fundamentals modules** covering core producer/consumer patterns, and a **real-time streaming pipeline** that ingests live Wikipedia edits from the Wikimedia EventStreams API, moves them through Kafka, and persists them in MongoDB.

🇬🇧 [English](#english) · 🇫🇷 [Français](#français)

---

<a name="english"></a>
## 🇬🇧 English

### Overview

This repository is split into three independent Spring Boot modules:

| Module | Role |
|---|---|
| `kafka-demo` | Fundamentals sandbox — String & JSON producers/consumers, topic configuration, REST-triggered publishing |
| `producer/producer` | Reactive producer — streams live Wikipedia edits and publishes them to Kafka |
| `consumer/consumer` | Consumer — subscribes to the stream and persists events to MongoDB |

### Architecture

```
Wikimedia EventStreams (SSE)
            │
            ▼
   ┌─────────────────┐
   │ Producer Service │  WebClient / WebFlux (reactive, non-blocking)
   │   (port 8082)    │
   └────────┬─────────┘
            │  publishes to
            ▼
   Kafka Topic: wikimedia-stream
            │
            ▼
   ┌─────────────────┐
   │ Consumer Service │  @KafkaListener
   └────────┬─────────┘
            │  persists to
            ▼
         MongoDB
```

The `kafka-demo` module is a separate, self-contained sandbox (topic `NadaJbr`) used to demonstrate the core Kafka concepts — String messages, JSON payload serialization, topic provisioning via `NewTopic` beans, and REST-triggered publishing — before applying them in the real pipeline above.

### Tech stack

- **Language / Runtime:** Java 17
- **Framework:** Spring Boot, Spring Kafka, Spring WebFlux (reactive `WebClient`)
- **Messaging:** Apache Kafka (topic provisioning via `TopicBuilder`/`NewTopic`)
- **Persistence:** MongoDB (consumer-side storage of streamed events)
- **Serialization:** Jackson (JSON producer/consumer, `spring.json.trusted.packages`)
- **Utilities:** Lombok
- **Build:** Maven (multi-module)

### What each module does

**`kafka-demo`** — fundamentals
- `kafkaTopicConfig`: provisions the `NadaJbr` topic
- `kafkaProducer` / `KafkaConsumer`: plain String messages
- `kafkaJsonProducer` / `KafkaConsumer#consumeJsonMsg`: JSON messages (`Student` payload) via `JsonSerializer`/`JsonDeserializer`
- `MessageController`: `POST /api/v1/messages` and `POST /api/v1/messages/json` to trigger publishing

**`producer/producer`** — real-time ingestion
- `WikimediaStreamConsumer`: opens a reactive `WebClient` stream against the Wikimedia EventStreams API (`stream.wikimedia.org/v2/stream/recentchange`) and forwards each event to Kafka
- `wikimediaProducer`: publishes messages to the `wikimedia-stream` topic
- `wikimediaTopicConfig`: provisions the `wikimedia-stream` topic
- `WikimediaController`: `GET /api/v1/wikimedia` starts the stream-to-Kafka pipeline

**`consumer/consumer`** — persistence
- `WikimediaConsumer`: `@KafkaListener` on `wikimedia-stream`
- Consumed events are persisted to **MongoDB**, making the full edit history queryable for downstream use (analytics, search, replay)

### Getting started

**Prerequisites**
- Java 17+
- Maven
- A running Kafka broker (`localhost:9092` by default)
- A running MongoDB instance

**Run the fundamentals demo**
```bash
cd kafka-demo
./mvnw spring-boot:run
```

**Run the real-time pipeline**
```bash
# Terminal 1 — consumer (persists to MongoDB)
cd consumer/consumer
./mvnw spring-boot:run

# Terminal 2 — producer (port 8082)
cd producer/producer
./mvnw spring-boot:run
```

Then trigger the stream:
```bash
curl http://localhost:8082/api/v1/wikimedia
```

Live Wikipedia edits will start flowing: Wikimedia → Producer → Kafka (`wikimedia-stream`) → Consumer → MongoDB.

### Project structure
```
.
├── kafka-demo/                 # Kafka fundamentals sandbox
├── producer/producer/          # Reactive Wikimedia → Kafka producer
└── consumer/consumer/          # Kafka → MongoDB consumer
```

---

<a name="français"></a>
## 🇫🇷 Français

### Aperçu

Ce dépôt est organisé en trois modules Spring Boot indépendants :

| Module | Rôle |
|---|---|
| `kafka-demo` | Bac à sable pédagogique — producteurs/consommateurs String & JSON, configuration de topics, publication déclenchée via REST |
| `producer/producer` | Producteur réactif — récupère en temps réel les modifications Wikipedia et les publie sur Kafka |
| `consumer/consumer` | Consommateur — s'abonne au flux et persiste les événements dans MongoDB |

### Architecture

```
Wikimedia EventStreams (SSE)
            │
            ▼
   ┌─────────────────┐
   │ Service Producer │  WebClient / WebFlux (réactif, non-bloquant)
   │   (port 8082)    │
   └────────┬─────────┘
            │  publie sur
            ▼
   Topic Kafka : wikimedia-stream
            │
            ▼
   ┌─────────────────┐
   │ Service Consumer │  @KafkaListener
   └────────┬─────────┘
            │  persiste dans
            ▼
         MongoDB
```

Le module `kafka-demo` est un bac à sable séparé (topic `NadaJbr`) qui illustre les concepts fondamentaux de Kafka — messages String, sérialisation JSON, provisioning de topics via des beans `NewTopic`, publication déclenchée par API REST — avant leur mise en application dans le pipeline réel ci-dessus.

### Stack technique

- **Langage / Runtime :** Java 17
- **Framework :** Spring Boot, Spring Kafka, Spring WebFlux (`WebClient` réactif)
- **Messagerie :** Apache Kafka (provisioning de topics via `TopicBuilder`/`NewTopic`)
- **Persistance :** MongoDB (stockage des événements consommés)
- **Sérialisation :** Jackson (producteur/consommateur JSON, `spring.json.trusted.packages`)
- **Utilitaires :** Lombok
- **Build :** Maven (multi-module)

### Démarrage

**Prérequis**
- Java 17+
- Maven
- Un broker Kafka actif (`localhost:9092` par défaut)
- Une instance MongoDB active

**Lancer la démo fondamentaux**
```bash
cd kafka-demo
./mvnw spring-boot:run
```

**Lancer le pipeline temps réel**
```bash
# Terminal 1 — consumer (persiste dans MongoDB)
cd consumer/consumer
./mvnw spring-boot:run

# Terminal 2 — producer (port 8082)
cd producer/producer
./mvnw spring-boot:run
```

Puis déclencher le flux :
```bash
curl http://localhost:8082/api/v1/wikimedia
```

Les modifications Wikipedia en direct commencent alors à circuler : Wikimedia → Producer → Kafka (`wikimedia-stream`) → Consumer → MongoDB.

### Structure du projet
```
.
├── kafka-demo/                 # Bac à sable fondamentaux Kafka
├── producer/producer/          # Producteur réactif Wikimedia → Kafka
└── consumer/consumer/          # Consommateur Kafka → MongoDB
```

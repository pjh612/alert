package com.alert.infra.kafka;


import com.alert.config.AlertKafkaProperties;
import com.alert.core.messaging.consumer.ReactiveMessageConsumerRegistrar;
import com.alert.core.messaging.consumer.ReactiveTopicAlertMessageHandler;
import com.alert.core.messaging.model.AlertMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class ReactiveKafkaMessageConsumerRegistrar<R> implements ReactiveMessageConsumerRegistrar<R>, SmartLifecycle {
    private final Map<String, Object> properties;
    private final AlertKafkaProperties.DlqProperties dlqProperties;
    private final KafkaSender<String, AlertMessage> kafkaSender;
    private final Map<String, Runnable> pendingRegistrations = new LinkedHashMap<>();
    private final Map<String, Disposable> topicDisposables = new LinkedHashMap<>();
    private volatile boolean running = false;

    private static final Logger log = LoggerFactory.getLogger(ReactiveKafkaMessageConsumerRegistrar.class);

    public ReactiveKafkaMessageConsumerRegistrar(Map<String, Object> properties,
                                                 AlertKafkaProperties.DlqProperties dlqProperties,
                                                 KafkaSender<String, AlertMessage> kafkaSender) {
        this.properties = properties;
        this.dlqProperties = dlqProperties;
        this.kafkaSender = kafkaSender;
    }

    @Override
    public synchronized void register(String topic, ReactiveTopicAlertMessageHandler<R> messageHandler, Integer concurrency) {
        pendingRegistrations.put(topic, () -> startConsumer(topic, messageHandler, concurrency));
        if (running) {
            disposeIfRunning(topic);
            startConsumer(topic, messageHandler, concurrency);
        }
    }

    private void disposeIfRunning(String topic) {
        Disposable old = topicDisposables.get(topic);
        if (old != null && !old.isDisposed()) {
            try {
                old.dispose();
            } catch (Exception e) {
                log.warn("Failed to dispose existing consumer for topic {}: {}", topic, e.getMessage());
            }
        }
    }

    private void startConsumer(String topic, ReactiveTopicAlertMessageHandler<R> messageHandler, Integer concurrency) {
        String dlqTopic = topic + dlqProperties.topicSuffix();
        ReceiverOptions<String, AlertMessage> receiverOptions = createReceiverOptions(topic);

        Disposable disposable = KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnSubscribe(s -> log.info("Successfully subscribed to Kafka topic: {}", topic))
                .doOnError(e -> log.error("Kafka consumer error on topic {}: {}", topic, e.getMessage()))
                .flatMap(record -> processMessage(record, messageHandler, dlqTopic), concurrency)
                .subscribe();

        topicDisposables.put(topic, disposable);
    }

    Mono<Void> processMessage(ReceiverRecord<String, AlertMessage> record, ReactiveTopicAlertMessageHandler<R> messageHandler, String dlqTopic) {
        if (record.value() == null) {
            log.error("Deserialization failed for record on topic={} partition={} offset={}, routing to DLQ",
                    record.topic(), record.partition(), record.offset());
            return publishToDlq(dlqTopic, record, new RuntimeException("Deserialization failed"))
                    .onErrorResume(dlqError -> {
                        log.error("Failed to send deserialization-failed record to DLQ {}: {}", dlqTopic, dlqError.getMessage());
                        return Mono.empty();
                    })
                    .then(record.receiverOffset().commit()
                            .retryWhen(Retry.backoff(dlqProperties.backoff().maxAttempts(), Duration.ofMillis(dlqProperties.backoff().interval())))
                    )
                    .onErrorResume(e -> {
                        log.error("Failed to commit offset for deserialization-failed record on topic {}: {}", record.topic(), e.getMessage());
                        return Mono.empty();
                    });
        }
        log.debug("Received message from topic {}: {}", record.topic(), record.value());
        return messageHandler.handle(record.topic(), record.value())
                .retryWhen(Retry.fixedDelay(dlqProperties.backoff().maxAttempts(), Duration.ofMillis(dlqProperties.backoff().interval())))
                .onErrorResume(e -> {
                    Throwable cause = Exceptions.unwrap(e);
                    log.error("Message processing failed after retries, sending to DLQ {}: {}", dlqTopic, cause.getMessage());
                    return publishToDlq(dlqTopic, record, cause)
                            .onErrorResume(dlqError -> {
                                log.error("Failed to send to DLQ {}: {}", dlqTopic, dlqError.getMessage());
                                return Mono.empty();
                            })
                            .then(Mono.<R>empty());
                })
                .then(record.receiverOffset().commit()
                        .retryWhen(Retry.backoff(dlqProperties.backoff().maxAttempts(), Duration.ofMillis(dlqProperties.backoff().interval())))
                )
                .onErrorResume(e -> {
                    log.error("Failed to commit offset for topic {}: {}", record.topic(), e.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> publishToDlq(String dlqTopic, ReceiverRecord<String, AlertMessage> record, Throwable cause) {
        String message = cause.getMessage() != null
                ? cause.getMessage()
                : cause.getClass().getName();
        ProducerRecord<String, AlertMessage> producerRecord = new ProducerRecord<>(dlqTopic, null, record.key(), record.value(), record.headers());
        producerRecord.headers().add(new RecordHeader("x-orig-exception-message", message.getBytes(StandardCharsets.UTF_8)));
        SenderRecord<String, AlertMessage, String> senderRecord = SenderRecord.create(producerRecord, record.key());

        return kafkaSender.send(Mono.just(senderRecord))
                .next()
                .flatMap(result -> {
                    if (result.exception() != null) {
                        return Mono.error(result.exception());
                    }
                    return Mono.empty();
                });
    }

    @Override
    public synchronized void start() {
        if (running) return;
        log.info("Starting ReactiveKafkaMessageConsumerRegistrar...");
        running = true;
        pendingRegistrations.values().forEach(Runnable::run);
    }

    @Override
    public synchronized void stop() {
        running = false;
        int total = topicDisposables.size();
        int disposed = 0;
        for (Disposable d : topicDisposables.values()) {
            try {
                d.dispose();
                disposed++;
            } catch (Exception e) {
                log.warn("Failed to dispose Kafka consumer: {}", e.getMessage());
            }
        }
        topicDisposables.clear();
        log.info("ReactiveKafkaMessageConsumerRegistrar stopped ({}/{} consumers dispose requested)", disposed, total);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private ReceiverOptions<String, AlertMessage> createReceiverOptions(String topic) {
        return ReceiverOptions.<String, AlertMessage>create(properties)
                .subscription(Collections.singleton(topic));
    }
}

package com.alert.infra.kafka;


import com.alert.core.messaging.consumer.ReactiveMessageConsumerRegistrar;
import com.alert.core.messaging.consumer.ReactiveTopicAlertMessageHandler;
import com.alert.core.messaging.model.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.Map;

public class ReactiveKafkaMessageConsumerRegistrar<R> implements ReactiveMessageConsumerRegistrar<R> {
    private final Map<String, Object> properties;
    private static final Logger log = LoggerFactory.getLogger(ReactiveKafkaMessageConsumerRegistrar.class);

    public ReactiveKafkaMessageConsumerRegistrar(Map<String, Object> properties) {
        this.properties = properties;
    }

    private ReceiverOptions<String, AlertMessage> createReceiverOptions(String topic) {
        return ReceiverOptions.<String, AlertMessage>create(properties)
                .subscription(Collections.singleton(topic));
    }

    public void register(String topic, ReactiveTopicAlertMessageHandler<R> messageHandler, Integer concurrency) {
        ReceiverOptions<String, AlertMessage> receiverOptions = createReceiverOptions(topic);

        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnSubscribe(s -> log.info("Successfully subscribed to Kafka topic: {}", topic)) // 구독 시작 확인
                .doOnError(e -> log.error("Kafka consumer error on topic {}: {}", topic, e.getMessage())) // 에러 확인
                .flatMap(record -> {
                    log.debug("Received message from topic {}: {}", record.topic(), record.value());
                    return messageHandler.handle(record.topic(), record.value())
                            .then(record.receiverOffset().commit())
                            .onErrorResume(e -> {
                                log.error("Failed to process message from topic {}: {}", topic, e.getMessage());
                                return Mono.empty();
                            });
                }, concurrency)
                .subscribe();
    }
}


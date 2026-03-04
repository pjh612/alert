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
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
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
import java.util.concurrent.ConcurrentHashMap;

public class ReactiveKafkaMessageConsumerRegistrar<R> implements ReactiveMessageConsumerRegistrar<R>, SmartLifecycle {
    private final Map<String, Object> properties;
    private final AlertKafkaProperties.DlqProperties dlqProperties;
    private final KafkaSender<String, AlertMessage> kafkaSender;

    private final Map<String, Runnable> pendingRegistrations = new LinkedHashMap<>();
    private final Map<String, Sinks.Empty<Void>> topicStopSinks = new ConcurrentHashMap<>();
    private final Map<String, Mono<Void>> topicCompletionMonos = new ConcurrentHashMap<>();

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
            signalStop(topic); // 기존 리스너가 있다면 종료 신호 전송
            startConsumer(topic, messageHandler, concurrency);
        }
    }

    private void signalStop(String topic) {
        Sinks.Empty<Void> sink = topicStopSinks.remove(topic);
        topicCompletionMonos.remove(topic);
        if (sink != null) {
            sink.tryEmitEmpty();
            log.info("Sent stop signal to Kafka topic: {}", topic);
        }
    }

    private void startConsumer(String topic, ReactiveTopicAlertMessageHandler<R> messageHandler, Integer concurrency) {
        String dlqTopic = topic + dlqProperties.topicSuffix();
        ReceiverOptions<String, AlertMessage> receiverOptions = createReceiverOptions(topic);

        Sinks.Empty<Void> stopSink = Sinks.empty();
        topicStopSinks.put(topic, stopSink);

        Mono<Void> completion = KafkaReceiver.create(receiverOptions)
                .receive()
                .takeUntilOther(stopSink.asMono())
                .doOnSubscribe(s -> log.info("Successfully subscribed to Kafka topic: {}", topic))
                .flatMap(record -> processMessage(record, messageHandler, dlqTopic), concurrency)
                .doOnError(e -> log.error("Critical Kafka stream error on topic {}: {}", topic, e.getMessage()))
                .then()
                .cache(); // 종료 상태 공유

        topicCompletionMonos.put(topic, completion);
        completion.subscribe();
    }

    Mono<Void> processMessage(ReceiverRecord<String, AlertMessage> record, ReactiveTopicAlertMessageHandler<R> messageHandler, String dlqTopic) {
        if (record.value() == null) {
            return handleFailure(record, dlqTopic, new RuntimeException("Deserialization failed"))
                    .then(commitOffset(record));
        }

        // 2. 메시지 처리 로직
        return messageHandler.handle(record.topic(), record.value()) // Mono<R> 반환
                .retryWhen(Retry.fixedDelay(dlqProperties.backoff().maxAttempts(), Duration.ofMillis(dlqProperties.backoff().interval())))
                .then()
                .onErrorResume(e -> handleFailure(record, dlqTopic, Exceptions.unwrap(e)))
                .then(commitOffset(record));
    }

    private Mono<Void> handleFailure(ReceiverRecord<String, AlertMessage> record, String dlqTopic, Throwable cause) {
        log.error("Message processing failed, sending to DLQ {}: {}", dlqTopic, cause.getMessage());
        return publishToDlq(dlqTopic, record, cause)
                .onErrorResume(dlqError -> {
                    log.error("Critical: Failed to send to DLQ {}: {}", dlqTopic, dlqError.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> commitOffset(ReceiverRecord<String, AlertMessage> record) {
        return record.receiverOffset().commit()
                .retryWhen(Retry.backoff(dlqProperties.backoff().maxAttempts(), Duration.ofMillis(dlqProperties.backoff().interval())))
                .doOnError(e -> log.error("Failed to commit offset for topic {}: {}", record.topic(), e.getMessage()))
                .onErrorComplete();
    }

    private Mono<Void> publishToDlq(String dlqTopic, ReceiverRecord<String, AlertMessage> record, Throwable cause) {
        String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getName();
        ProducerRecord<String, AlertMessage> producerRecord = new ProducerRecord<>(dlqTopic, null, record.key(), record.value(), record.headers());
        producerRecord.headers().add(new RecordHeader("x-orig-exception-message", message.getBytes(StandardCharsets.UTF_8)));

        return kafkaSender.send(Mono.just(SenderRecord.create(producerRecord, record.key())))
                .next()
                .flatMap(result -> result.exception() != null ? Mono.error(result.exception()) : Mono.empty());
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
        if (!running) return;
        running = false;

        int total = topicStopSinks.size();
        log.info("Stopping ReactiveKafkaMessageConsumerRegistrar... Draining {} listeners", total);

        topicStopSinks.values().forEach(Sinks.Empty::tryEmitEmpty);

        try {
            Mono.when(topicCompletionMonos.values())
                    .block(Duration.ofSeconds(30));
            log.info("All Kafka consumers drained successfully.");
        } catch (Exception e) {
            log.warn("Timeout or error while draining Kafka consumers: {}", e.getMessage());
        }

        topicStopSinks.clear();
        topicCompletionMonos.clear();
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
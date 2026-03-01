package com.alert.infra.kafka;

import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.publisher.ReactiveAlertMessagePublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

public class ReactiveKafkaAlertMessagePublisher implements ReactiveAlertMessagePublisher {
    private static final Logger log = LoggerFactory.getLogger(ReactiveKafkaAlertMessagePublisher.class);

    private final ReactiveAlertCacheManager alertCacheManager;
    private final AlertMessageSupport support;
    private final KafkaSender<String, AlertMessage> kafkaSender;
    private final PartitionKeyStrategy partitionKeyStrategy;

    public ReactiveKafkaAlertMessagePublisher(ReactiveAlertCacheManager alertCacheManager, AlertMessageSupport support, KafkaSender<String, AlertMessage> kafkaSender, PartitionKeyStrategy partitionKeyStrategy) {
        this.alertCacheManager = alertCacheManager;
        this.support = support;
        this.kafkaSender = kafkaSender;
        this.partitionKeyStrategy = partitionKeyStrategy;
    }

    @Override
    public Mono<Void> publish(String channel, AlertMessage message) {
        return Mono.defer(() -> {
            String eventId = message.id();
            String partitionKey = partitionKeyStrategy.resolve(message);

            log.debug("Publishing alert. channel:{}, eventId:{}, message:{}", channel, eventId, message);

            return saveToCache(message, eventId)
                    .then(sendToKafka(channel, partitionKey, message, eventId));
        });
    }

    private Mono<Void> saveToCache(AlertMessage message, String eventId) {
        if (!message.type().isCacheable()) {
            return Mono.empty();
        }

        return Flux.fromIterable(message.targets())
                .flatMap(target -> {
                    String cacheKey = support.resolveCacheKey(message.namespace(), target);
                    return alertCacheManager.save(cacheKey, eventId, message);
                })
                .then()
                .onErrorResume(e -> {
                    log.error("Cache save failed for eventId: {}", eventId, e);
                    return Mono.empty();
                });
    }

    private Mono<Void> sendToKafka(String channel, String partitionKey, AlertMessage message, String eventId) {
        return kafkaSender.send(Mono.just(SenderRecord.create(new ProducerRecord<>(channel, partitionKey, message), eventId)))
                .next() // 결과가 하나이므로 next()로 Mono 전환
                .flatMap(result -> {
                    if (result.exception() != null) {
                        return Mono.error(result.exception());
                    }
                    return Mono.empty();
                })
                .doOnTerminate(() -> log.debug("Completed Kafka publish attempt for eventId: {}", eventId))
                .then();
    }
}

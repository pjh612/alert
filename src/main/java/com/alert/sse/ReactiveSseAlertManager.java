package com.alert.sse;

import com.alert.cache.ReactiveAlertCacheManager;
import com.alert.core.manager.ReactiveAbstractAlertManager;
import com.alert.core.messaging.bridge.ReactiveAlertMessagePublisher;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.manager.ReactiveSubscribableAlertManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;

public class ReactiveSseAlertManager extends ReactiveAbstractAlertManager implements ReactiveSubscribableAlertManager<Flux<ServerSentEvent<Object>>> {
    private final ReactiveEmitterRepository reactiveEmitterRepository;
    private final ReactiveAlertCacheManager alertCacheManager;
    private final Class<? extends AlertMessage> messageType;

    private static final Logger log = LoggerFactory.getLogger(ReactiveSseAlertManager.class);
    private static final String ALERT_KEY_FORMAT = "alert:%s";

    public ReactiveSseAlertManager(ReactiveAlertMessagePublisher alertMessagePublisher, AlertMessageFactory alertMessageFactory, ReactiveEmitterRepository reactiveEmitterRepository, ReactiveAlertCacheManager alertCacheManager, Class<? extends AlertMessage> messageType) {
        super(alertMessageFactory, alertMessagePublisher);
        this.reactiveEmitterRepository = reactiveEmitterRepository;
        this.alertCacheManager = alertCacheManager;
        this.messageType = messageType;
    }

    @Override
    public Flux<ServerSentEvent<Object>> subscribe(AlertChannel alertChannel, String subscriberId, String lastEventId, Long timeoutMillis) {
        Sinks.Many<ServerSentEvent<Object>> sink = reactiveEmitterRepository.put(subscriberId);
        AlertMessage alertMessage = alertMessageFactory.onConnect(subscriberId);

        return alertMessagePublisher.publish(alertChannel.name(), alertMessage)
                .then(isReconnected(lastEventId)
                        ? republishMissedMessages(alertChannel, subscriberId, lastEventId).then()
                        : Mono.empty())
                .thenMany(sink.asFlux())
                .doOnCancel(() -> cleanUpSink(subscriberId))
                .doOnError(e -> log.error("SSE error for subscriber {}: {}", subscriberId, e.getMessage()))
                .onErrorResume(e -> {
                    sink.tryEmitNext(ServerSentEvent.builder()
                            .event("error")
                            .data("An error occurred: " + e.getMessage())
                            .build());
                    return Mono.empty();
                })
                .take(Duration.ofMillis(timeoutMillis))
                .doFinally(signalType -> {
                    log.info("Stream terminated for subscriber {} with signal {}", subscriberId, signalType);
                    cleanUpSink(subscriberId);
                });
    }

    private boolean isReconnected(String lastEventId) {
        return lastEventId != null && !lastEventId.isEmpty();
    }

    private Mono<Void> republishMissedMessages(AlertChannel alertChannel, String subscriberId, String lastEventId) {
        String key = String.format(ALERT_KEY_FORMAT, subscriberId);
        long offset = parseOffset(lastEventId);

        return offset < 0
                ? Mono.empty()
                : alertCacheManager.getFromOffset(key, offset, messageType)
                .map(it -> alertMessageFactory.onReplayMessage(subscriberId, it))
                .flatMap(it -> alertMessagePublisher.publish(alertChannel.name(), it))
                .then();
    }

    private long parseOffset(String lastEventId) {
        try {
            return Long.parseLong(lastEventId);
        } catch (NumberFormatException e) {
            log.warn("Invalid lastEventId format: '{}'. Skipping message replay.", lastEventId);
            return -1;
        }
    }

    private void cleanUpSink(String subscriberId) {
        reactiveEmitterRepository.deleteById(subscriberId);
    }
}

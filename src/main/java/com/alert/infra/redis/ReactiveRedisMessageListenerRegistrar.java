package com.alert.infra.redis;

import com.alert.core.messaging.broadcaster.MessageConverter;
import com.alert.core.messaging.broadcaster.ReactiveAlertMessageHandler;
import com.alert.core.messaging.broadcaster.ReactiveAlertMessageListenerRegistrar;
import com.alert.core.messaging.model.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public class ReactiveRedisMessageListenerRegistrar
        implements ReactiveAlertMessageListenerRegistrar<String>, SmartLifecycle {

    private final ReactiveRedisMessageListenerContainer container;
    private final Map<String, Runnable> pendingRegistrations = new LinkedHashMap<>();
    private final Map<String, Sinks.Empty<Void>> topicStopSinks = new LinkedHashMap<>();
    private final Map<String, Mono<Void>> topicCompletionMonos = new LinkedHashMap<>();
    private volatile boolean running = false;

    private static final Logger log = LoggerFactory.getLogger(ReactiveRedisMessageListenerRegistrar.class);

    public ReactiveRedisMessageListenerRegistrar(ReactiveRedisMessageListenerContainer container) {
        this.container = container;
    }

    @Override
    public synchronized void register(String topic, ReactiveAlertMessageHandler handler,
                                      MessageConverter<String, ? extends AlertMessage> messageConverter) {
        pendingRegistrations.put(topic, () -> startListener(topic, handler, messageConverter));
        if (running) {
            signalStop(topic);
            startListener(topic, handler, messageConverter);
        }
    }

    private void signalStop(String topic) {
        Sinks.Empty<Void> sink = topicStopSinks.remove(topic);
        topicCompletionMonos.remove(topic);
        if (sink != null) {
            sink.tryEmitEmpty();
        }
    }

    private void startListener(String topic, ReactiveAlertMessageHandler handler,
                               MessageConverter<String, ? extends AlertMessage> messageConverter) {
        Sinks.Empty<Void> stopSink = Sinks.empty();
        topicStopSinks.put(topic, stopSink);

        Mono<Void> completion = container.receive(ChannelTopic.of(topic))
                .takeUntilOther(stopSink.asMono())
                .flatMap(it -> {
                    AlertMessage alertMessage = messageConverter.convert(it.getMessage());
                    return handler.handle(alertMessage);
                })
                .doOnError(e -> log.error("Error in reactive redis listener for topic {}: {}", topic, e.getMessage()))
                .then()
                .onErrorComplete()
                .cache();

        topicCompletionMonos.put(topic, completion);
        completion.subscribe();
        log.info("Successfully subscribed to Redis topic: {}", topic);
    }

    @Override
    public synchronized void start() {
        if (running) return;
        log.info("Starting ReactiveRedisMessageListenerRegistrar...");
        running = true;
        pendingRegistrations.values().forEach(Runnable::run);
    }

    @Override
    public synchronized void stop() {
        running = false;
        int total = topicStopSinks.size();

        topicStopSinks.values().forEach(Sinks.Empty::tryEmitEmpty);

        // 2) 모든 flatMap 내 in-flight 메시지가 처리될 때까지 대기
        try {
            Mono.when(topicCompletionMonos.values())
                    .block(Duration.ofSeconds(30));
        } catch (Exception e) {
            log.warn("Timeout or error waiting for Redis listeners to drain: {}", e.getMessage());
        }

        topicStopSinks.clear();
        topicCompletionMonos.clear();
        log.info("ReactiveRedisMessageListenerRegistrar stopped ({} listeners drained)", total);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }
}

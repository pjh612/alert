package com.alert.core.messaging.consumer;

import com.alert.core.messaging.broadcaster.ReactiveMessageBroadcaster;
import com.alert.core.messaging.model.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

public class ReactiveAlertMessageBroadcastHandler<R> implements ReactiveTopicAlertMessageHandler<R> {
    private final ReactiveMessageBroadcaster<String, R> messageBroadcaster;
    private final ObjectMapper objectMapper;

    private static final Logger log = LoggerFactory.getLogger(ReactiveAlertMessageBroadcastHandler.class);

    public ReactiveAlertMessageBroadcastHandler(ReactiveMessageBroadcaster<String, R> messageBroadcaster, ObjectMapper objectMapper) {
        this.messageBroadcaster = messageBroadcaster;
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<R> handle(String topic, AlertMessage message) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(message))
                .flatMap(json -> {
                    if (log.isTraceEnabled()) {
                        log.trace("[AlertMessageBroadcastHandler] handle message {}", json);
                    }
                    return messageBroadcaster.sendMessage(topic, json);
                })
                .doOnError(e -> log.error("Failed to send notification via broadcaster: {}", e.getMessage()));
    }
}

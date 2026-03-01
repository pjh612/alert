package com.alert.core.messaging.consumer;

import com.alert.core.messaging.broadcaster.ReactiveMessageBroadcaster;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.BasicAlertMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.ObjectMapper;

public class ReactiveAlertMessageBroadcastHandler<R> implements ReactiveTopicAlertMessageHandler<R> {
    private final ReactiveMessageBroadcaster<String, R> messageBroadcaster;
    private final ObjectMapper objectMapper;
    private final BasicAlertMessageSender basicSender;

    private static final Logger log = LoggerFactory.getLogger(ReactiveAlertMessageBroadcastHandler.class);

    public ReactiveAlertMessageBroadcastHandler(ReactiveMessageBroadcaster<String, R> messageBroadcaster, ObjectMapper objectMapper, BasicAlertMessageSender basicSender) {
        this.messageBroadcaster = messageBroadcaster;
        this.objectMapper = objectMapper;
        this.basicSender = basicSender;
    }

    @Override
    public Mono<R> handle(String topic, AlertMessage message) {
        Mono<Void> basicStep = basicSender != null
                ? Mono.fromRunnable(() -> basicSender.send(message.namespace(), message.id(), message))
                        .subscribeOn(Schedulers.boundedElastic()).then()
                : Mono.empty();

        return basicStep
                .then(Mono.fromCallable(() -> objectMapper.writeValueAsString(message)))
                .flatMap(json -> {
                    if (log.isTraceEnabled()) {
                        log.trace("[ReactiveAlertMessageBroadcastHandler] handle message {}", json);
                    }
                    return messageBroadcaster.sendMessage(topic, json);
                })
                .doOnError(e -> log.error("Failed to send notification via broadcaster: {}", e.getMessage()));
    }
}

package com.alert.core.messaging.consumer;

import com.alert.core.messaging.model.AlertMessage;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface ReactiveTopicAlertMessageHandler<R> {
    Mono<R> handle(String topic, AlertMessage message);
}

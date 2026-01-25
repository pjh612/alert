package com.alert.core.messaging.broadcaster;

import reactor.core.publisher.Mono;

@FunctionalInterface
public interface ReactiveMessageBroadcaster<T, R> {
    Mono<R> sendMessage(String topic, T message);
}

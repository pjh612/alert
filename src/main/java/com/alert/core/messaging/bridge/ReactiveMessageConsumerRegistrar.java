package com.alert.core.messaging.bridge;

public interface ReactiveMessageConsumerRegistrar<R> {
    void register(String topic, ReactiveTopicAlertMessageHandler<R> messageHandler, Integer concurrency);
}

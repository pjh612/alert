package com.alert.core.messaging.consumer;

public interface ReactiveMessageConsumerRegistrar<R> {
    void register(String topic, ReactiveTopicAlertMessageHandler<R> messageHandler, Integer concurrency);
}

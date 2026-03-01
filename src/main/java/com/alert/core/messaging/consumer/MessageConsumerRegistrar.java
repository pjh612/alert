package com.alert.core.messaging.consumer;

public interface MessageConsumerRegistrar {
    void register(String topic, TopicAlertMessageHandler messageHandler);
}

package com.alert.core.messaging.bridge;

public interface MessageConsumerRegistrar {
    void register(String topic, TopicAlertMessageHandler messageHandler);
}

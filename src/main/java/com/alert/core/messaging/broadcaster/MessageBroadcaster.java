package com.alert.core.messaging.broadcaster;

@FunctionalInterface
public interface MessageBroadcaster<T> {
    void sendMessage(String topic, T message);
}

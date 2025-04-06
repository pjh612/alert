package com.alert.core.messaging.bridge;

import com.alert.core.messaging.model.AlertMessage;

public interface MessagePublisher<T extends AlertMessage> {
    void publish(String channel, T message);
}

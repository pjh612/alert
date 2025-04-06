package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;

public interface MessageListenerRegistrar<T extends AlertMessage> {
    void register(String topic, AlertMessageHandler<T> listener, MessageConverter<byte[], T> messageConverter);
}

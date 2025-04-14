package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;

public interface AlertMessageListenerRegistrar<T> {
    void register(String topic, AlertMessageHandler handler, MessageConverter<T, ? extends AlertMessage> messageConverter);
}

package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;

public interface ReactiveAlertMessageListenerRegistrar<T> {
    void register(String topic, ReactiveAlertMessageHandler handler, MessageConverter<T, ? extends AlertMessage> messageConverter);
}

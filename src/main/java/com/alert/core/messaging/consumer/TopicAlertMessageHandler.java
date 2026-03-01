package com.alert.core.messaging.consumer;

import com.alert.core.messaging.model.AlertMessage;

@FunctionalInterface
public interface TopicAlertMessageHandler {
    void handle(String topic, AlertMessage message);
}

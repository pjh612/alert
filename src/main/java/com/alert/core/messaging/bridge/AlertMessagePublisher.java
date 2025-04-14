package com.alert.core.messaging.bridge;

import com.alert.core.messaging.model.AlertMessage;

public interface AlertMessagePublisher {
    void publish(String channel, AlertMessage message);
}

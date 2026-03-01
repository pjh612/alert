package com.alert.core.messaging.publisher;

import com.alert.core.messaging.model.AlertMessage;

public interface AlertMessagePublisher {
    void publish(String channel, AlertMessage message);
}

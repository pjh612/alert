package com.alert.core.messaging.sender;

import com.alert.core.messaging.model.AlertMessage;

public interface AlertMessageSender {
    void send(String id, AlertMessage message);
}

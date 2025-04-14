package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;

public interface AlertMessageHandler {

    void handle(AlertMessage message);
}

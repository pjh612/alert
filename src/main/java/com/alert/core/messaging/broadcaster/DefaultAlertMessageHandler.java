package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.FanoutAlertMessageSender;

public class DefaultAlertMessageHandler implements AlertMessageHandler {
    private final FanoutAlertMessageSender alertMessageSender;

    public DefaultAlertMessageHandler(FanoutAlertMessageSender alertMessageSender) {
        this.alertMessageSender = alertMessageSender;
    }

    @Override
    public void handle(AlertMessage message) {
        alertMessageSender.send(message.namespace(), message.id(), message);
    }
}

package com.alert.core.messaging.sender;

import com.alert.core.messaging.model.AlertMessage;

import java.util.List;

public class AlertMessageDelegateSender implements AlertMessageSender {
    private final List<AlertMessageSender> alertMessageSenders;

    public AlertMessageDelegateSender(List<AlertMessageSender> alertMessageSenders) {
        this.alertMessageSenders = alertMessageSenders;
    }

    @Override
    public void send(String namespace, String id, AlertMessage message) {
        alertMessageSenders.forEach(alertMessageSender -> alertMessageSender.send(namespace, id, message));
    }
}

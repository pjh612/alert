package com.alert.core.messaging.model;

public class DefaultAlertMessageFactory implements AlertMessageFactory<DefaultAlertMessage> {

    @Override
    public DefaultAlertMessage onConnect(String targetId) {
        return new DefaultAlertMessage(targetId, DefaultAlertMessageType.CONNECT, "connected", false);
    }

    @Override
    public DefaultAlertMessage onReplayMessage(String targetId, Object message) {
        return new DefaultAlertMessage(targetId, DefaultAlertMessageType.MESSAGE, message, false);
    }

    @Override
    public DefaultAlertMessage onMessage(String targetId, Object message) {
        return new DefaultAlertMessage(targetId, DefaultAlertMessageType.MESSAGE, message, false);
    }
}

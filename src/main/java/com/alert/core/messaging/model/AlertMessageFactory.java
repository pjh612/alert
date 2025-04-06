package com.alert.core.messaging.model;

public interface AlertMessageFactory<T extends AlertMessage> {

    T onConnect(String targetId);

    T onReplayMessage(String targetId, Object message);

    T onMessage(String targetId, Object message);
}

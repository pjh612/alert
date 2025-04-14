package com.alert.core.messaging.model;

public interface AlertMessageFactory {

    AlertMessage onConnect(String targetId);

    AlertMessage onReplayMessage(String targetId, Object message);

    AlertMessage onMessage(String targetId, Object message);
}

package com.alert.core.messaging.model;

public interface AlertMessage {
    AlertMessageType type();

    String targetId();

    Object body();

    boolean isReplay();
}

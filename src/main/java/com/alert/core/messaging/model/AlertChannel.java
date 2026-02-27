package com.alert.core.messaging.model;

public interface AlertChannel {
    String name();

    default String namespace() {
        return name();
    }
}

package com.alert.core.messaging.model;

public interface AlertMessageType {
    String type();

    boolean isCacheable();

    String toString();
}

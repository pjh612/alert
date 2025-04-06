package com.alert.core.messaging.model;

public enum DefaultAlertMessageType implements AlertMessageType {
    CONNECT("connect"),
    MESSAGE("message");

    private final String type;

    DefaultAlertMessageType(String type) {
        this.type = type;
    }

    @Override
    public String type() {
        return this.type;
    }

    @Override
    public boolean isCacheable() {
        return this == MESSAGE;
    }
}

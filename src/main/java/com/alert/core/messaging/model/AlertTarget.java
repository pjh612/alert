package com.alert.core.messaging.model;

public record AlertTarget(TargetType type, String value) {

    public static AlertTarget id(String id) {
        return new AlertTarget(TargetType.ID, id);
    }

    public static AlertTarget tag(String tag) {
        return new AlertTarget(TargetType.TAG, tag);
    }

    public enum TargetType {
        ID,
        TAG
    }
}

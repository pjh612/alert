package com.alert.core.messaging.model;

public record DefaultAlertMessage(String targetId, DefaultAlertMessageType type, Object body, boolean isReplay) implements AlertMessage {
}

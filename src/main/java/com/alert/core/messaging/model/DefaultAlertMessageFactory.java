package com.alert.core.messaging.model;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DefaultAlertMessageFactory implements AlertMessageFactory {

    @Override
    public AlertMessage onConnect(String subscriberId, Map<String, String> attributes) {
        return new DefaultAlertMessage(
                UUID.randomUUID().toString(),
                List.of(AlertTarget.id(subscriberId)),
                DefaultAlertMessageType.CONNECT,
                "connected",
                false,
                attributes
        );
    }

    @Override
    public AlertMessage onReplay(String subscriberId, AlertMessage original, Map<String, String> attributes) {
        return new DefaultAlertMessage(
                UUID.randomUUID().toString(),
                List.of(AlertTarget.id(subscriberId)),
                original.type(),
                original.body(),
                true,
                attributes
        );
    }

    @Override
    public AlertMessage create(List<AlertTarget> targets, AlertMessageType type, Object body, Map<String, String> attributes) {
        return new DefaultAlertMessage(UUID.randomUUID().toString(),
                targets, type,
                body,
                false,
                attributes);
    }
}

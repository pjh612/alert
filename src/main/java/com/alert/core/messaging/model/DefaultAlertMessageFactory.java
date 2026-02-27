package com.alert.core.messaging.model;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DefaultAlertMessageFactory implements AlertMessageFactory {

    @Override
    public AlertMessage onConnect(String namespace, String subscriberId, Map<String, String> attributes) {
        return new DefaultAlertMessage(
                UUID.randomUUID().toString(),
                namespace,
                List.of(AlertTarget.id(subscriberId)),
                DefaultAlertMessageType.CONNECT,
                "connected",
                false,
                attributes
        );
    }

    @Override
    public AlertMessage onReplay(String namespace, String subscriberId, AlertMessage original, Map<String, String> attributes) {
        return new DefaultAlertMessage(
                UUID.randomUUID().toString(),
                namespace,
                List.of(AlertTarget.id(subscriberId)),
                original.type(),
                original.body(),
                true,
                attributes
        );
    }

    @Override
    public AlertMessage create(String namespace, List<AlertTarget> targets, AlertMessageType type, Object body, Map<String, String> attributes) {
        return new DefaultAlertMessage(
                UUID.randomUUID().toString(),
                namespace,
                targets,
                type,
                body,
                false,
                attributes
        );
    }
}

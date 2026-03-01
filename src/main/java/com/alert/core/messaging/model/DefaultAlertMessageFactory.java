package com.alert.core.messaging.model;

import com.alert.core.messaging.broadcaster.AlertMessageSupport;

import java.util.List;
import java.util.Map;

public class DefaultAlertMessageFactory implements AlertMessageFactory {
    private final AlertMessageSupport support;

    public DefaultAlertMessageFactory(AlertMessageSupport support) {
        this.support = support;
    }

    @Override
    public AlertMessage onConnect(String namespace, String subscriberId, Map<String, String> attributes) {
        return new DefaultAlertMessage(
                support.generateMessageId(),
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
                support.generateMessageId(),
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
                support.generateMessageId(),
                namespace,
                targets,
                type,
                body,
                false,
                attributes
        );
    }
}

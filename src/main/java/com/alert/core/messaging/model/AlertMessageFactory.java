package com.alert.core.messaging.model;

import java.util.List;
import java.util.Map;

public interface AlertMessageFactory {

    AlertMessage onConnect(String namespace,String subscriberId, Map<String, String> attributes);

    AlertMessage onReplay(String namespace, String subscriberId, AlertMessage original, Map<String, String> attributes);

    AlertMessage create(String namespace, List<AlertTarget> targets, AlertMessageType type, Object body, Map<String, String> attributes);
}

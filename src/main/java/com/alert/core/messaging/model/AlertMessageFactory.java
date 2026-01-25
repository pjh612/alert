package com.alert.core.messaging.model;

import java.util.List;
import java.util.Map;

public interface AlertMessageFactory {

    AlertMessage onConnect(String subscriberId, Map<String, String> attributes);

    AlertMessage onReplay(String subscriberId, AlertMessage original, Map<String, String> attributes);

    AlertMessage create(List<AlertTarget> targets, AlertMessageType type, Object body, Map<String, String> attributes);
}

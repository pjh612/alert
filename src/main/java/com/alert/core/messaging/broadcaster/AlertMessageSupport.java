package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertTarget;

public class AlertMessageSupport {
    private static final String ALERT_CACHE_KEY_FORMAT = "alert:%s:%s:%s";

    public String generateMessageId() {
        return Long.toString(System.currentTimeMillis());
    }

    public String resolveCacheKey(String topic, AlertTarget target) {
        String typePrefix = (target.type() == AlertTarget.TargetType.TAG) ? "tag" : "user";
        return ALERT_CACHE_KEY_FORMAT.formatted(topic, typePrefix, target.value());
    }
}

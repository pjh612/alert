package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.id.IdGenerator;
import com.alert.core.messaging.model.AlertTarget;

public class AlertMessageSupport {
    private static final String ALERT_CACHE_KEY_FORMAT = "alert:%s:%s:%s";
    private static final String ALERT_OFFSET_KEY_FORMAT = "alert:offset:%s:%s";

    private final IdGenerator<?> idGenerator;

    public AlertMessageSupport(IdGenerator<?> idGenerator) {
        this.idGenerator = idGenerator;
    }

    public String generateMessageId() {
        return String.valueOf(idGenerator.nextId());
    }

    public String resolveCacheKey(String namespace, AlertTarget target) {
        String typePrefix = switch (target.type()) {
            case TAG -> "tag";
            case BROADCAST -> "broadcast";
            case ID -> "user";
        };
        return ALERT_CACHE_KEY_FORMAT.formatted(namespace, typePrefix, target.value());
    }

    public String resolveOffsetKey(String namespace, String subscriberId) {
        return ALERT_OFFSET_KEY_FORMAT.formatted(namespace, subscriberId);
    }
}

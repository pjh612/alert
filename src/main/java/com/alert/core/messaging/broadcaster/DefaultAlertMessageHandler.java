package com.alert.core.messaging.broadcaster;

import com.alert.cache.AlertCacheManager;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;

public class DefaultAlertMessageHandler implements AlertMessageHandler {
    private final AlertCacheManager alertCacheManager;
    private final AlertMessageSender alertMessageSender;
    private final String topic;
    private static final String ALERT_CACHE_KEY_FORMAT = "alert:%s:%s";

    public DefaultAlertMessageHandler(AlertCacheManager alertCacheManager, AlertMessageSender alertMessageSender, String topic) {
        this.alertCacheManager = alertCacheManager;
        this.alertMessageSender = alertMessageSender;
        this.topic = topic;
    }

    @Override
    public void handle(AlertMessage message) {
        long currentTimeMillis = System.currentTimeMillis();
        String id = Long.toString(currentTimeMillis);
        alertMessageSender.send(message.targetId(), message);

        if (message.type().isCacheable()) {
            String key = ALERT_CACHE_KEY_FORMAT.formatted(topic, message.targetId());
            alertCacheManager.save(key, id, message);
        }
    }
}

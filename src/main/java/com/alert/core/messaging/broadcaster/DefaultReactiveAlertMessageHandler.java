package com.alert.core.messaging.broadcaster;

import com.alert.cache.ReactiveAlertCacheManager;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultReactiveAlertMessageHandler implements AlertMessageHandler {
    private final ReactiveAlertCacheManager alertCacheManager;
    private final AlertMessageSender alertMessageSender;
    private final String topic;

    private static final String ALERT_CACHE_KEY_FORMAT = "alert:%s:%s";
    private static final Logger log = LoggerFactory.getLogger(DefaultReactiveAlertMessageHandler.class);

    public DefaultReactiveAlertMessageHandler(ReactiveAlertCacheManager alertCacheManager, AlertMessageSender alertMessageSender, String topic) {
        this.alertCacheManager = alertCacheManager;
        this.alertMessageSender = alertMessageSender;
        this.topic = topic;
    }

    @Override
    public void handle(AlertMessage message) {
        long currentTimeMillis = System.currentTimeMillis();
        String id = Long.toString(currentTimeMillis);
        alertMessageSender.send(id, message);

        if (message.type().isCacheable()) {
            String key = ALERT_CACHE_KEY_FORMAT.formatted(topic, message.targetId());
            alertCacheManager.save(key, id, message)
                    .doOnTerminate(() -> log.debug("Cache save completed for key: {}", key))
                    .subscribe();
        }
    }
}

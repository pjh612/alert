package com.alert.core.messaging.broadcaster;

import com.alert.cache.AlertCacheManager;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;

public class DefaultAlertMessageHandler implements AlertMessageHandler {
    private final AlertCacheManager alertCacheManager;
    private final AlertMessageSender alertMessageSender;
    private final AlertMessageSupport support;
    private final String topic;

    public DefaultAlertMessageHandler(AlertCacheManager alertCacheManager,
                                      AlertMessageSender alertMessageSender,
                                      AlertMessageSupport support, String topic) {
        this.alertCacheManager = alertCacheManager;
        this.alertMessageSender = alertMessageSender;
        this.support = support;
        this.topic = topic;
    }

    @Override
    public void handle(AlertMessage message) {
        String msgId = support.generateMessageId();

        alertMessageSender.send(msgId, message);

        if (message.type().isCacheable()) {
            message.targets().forEach(target -> {
                String key = support.resolveCacheKey(topic, target);
                alertCacheManager.save(key, msgId, message);
            });
        }
    }
}
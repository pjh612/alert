package com.alert.core.messaging.broadcaster;

import com.alert.cache.AlertCacheManager;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;

public class DefaultAlertMessageHandler implements AlertMessageHandler {
    private final AlertCacheManager alertCacheManager;
    private final AlertMessageSender alertMessageSender;
    private final AlertMessageSupport support;

    public DefaultAlertMessageHandler(AlertCacheManager alertCacheManager,
                                      AlertMessageSender alertMessageSender,
                                      AlertMessageSupport support) {
        this.alertCacheManager = alertCacheManager;
        this.alertMessageSender = alertMessageSender;
        this.support = support;
    }

    @Override
    public void handle(AlertMessage message) {
        String msgId = support.generateMessageId();
        String namespace = message.namespace();

        alertMessageSender.send(namespace, msgId, message);

        if (message.type().isCacheable()) {
            message.targets().forEach(target -> {
                String key = support.resolveCacheKey(namespace, target);
                alertCacheManager.save(key, msgId, message);
            });
        }
    }
}

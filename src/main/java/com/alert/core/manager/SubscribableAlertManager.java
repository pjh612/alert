package com.alert.core.manager;

import com.alert.core.messaging.model.AlertChannel;

public interface SubscribableAlertManager<T> extends AlertManager {
    T subscribe(AlertChannel alertChannel, String subscriberId, String lastEventId, Long timeoutMillis);
}

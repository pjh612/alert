package com.alert.core.manager;


import com.alert.core.messaging.model.AlertChannel;

public interface ReactiveSubscribableAlertManager<T> extends ReactiveAlertManager {
    T subscribe(AlertChannel alertChannel, String subscriberId, String lastEventId, Long timeoutMillis);
}

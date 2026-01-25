package com.alert.core.manager;


import com.alert.core.messaging.model.AlertChannel;

import java.util.List;

public interface ReactiveSubscribableAlertManager<T> extends ReactiveAlertManager {
    T subscribe(
            AlertChannel alertChannel,
            String subscriberId,
            List<String> tags,
            String lastEventId,
            Long timeoutMillis
    );
}

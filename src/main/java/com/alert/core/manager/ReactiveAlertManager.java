package com.alert.core.manager;

import com.alert.core.messaging.model.AlertChannel;
import reactor.core.publisher.Mono;

public interface ReactiveAlertManager {
    Mono<Void> notice(AlertChannel alertChannel, String targetId, Object message);
}

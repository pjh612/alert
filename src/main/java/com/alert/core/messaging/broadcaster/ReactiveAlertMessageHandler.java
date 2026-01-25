package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;
import reactor.core.publisher.Mono;

public interface ReactiveAlertMessageHandler {
    Mono<Void> handle(AlertMessage message);
}

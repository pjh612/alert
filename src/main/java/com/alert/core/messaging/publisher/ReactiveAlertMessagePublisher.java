package com.alert.core.messaging.publisher;

import com.alert.core.messaging.model.AlertMessage;
import reactor.core.publisher.Mono;

public interface ReactiveAlertMessagePublisher {
    Mono<Void> publish(String channel, AlertMessage message);
}

package com.alert.core.manager;

import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.bridge.ReactiveAlertMessagePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public abstract class ReactiveAbstractAlertManager implements ReactiveAlertManager {
    protected final ReactiveAlertMessagePublisher alertMessagePublisher;
    protected final AlertMessageFactory alertMessageFactory;

    private static final Logger log = LoggerFactory.getLogger(ReactiveAbstractAlertManager.class);

    public ReactiveAbstractAlertManager(AlertMessageFactory alertMessageFactory, ReactiveAlertMessagePublisher alertMessagePublisher) {
        this.alertMessageFactory = alertMessageFactory;
        this.alertMessagePublisher = alertMessagePublisher;
    }

    @Override
    public Mono<Void> notice(AlertChannel alertChannel, String targetId, Object message) {
        AlertMessage alertMessage = this.alertMessageFactory.onMessage(targetId, message);
        log.debug("alert message published: {}", alertMessage);
        return alertMessagePublisher.publish(alertChannel.name(), alertMessage);
    }
}

package com.alert.core.manager;

import com.alert.core.messaging.publisher.ReactiveAlertMessagePublisher;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.List;

public abstract class ReactiveAbstractAlertManager implements ReactiveAlertManager {
    protected final ReactiveAlertMessagePublisher alertMessagePublisher;
    protected final AlertMessageFactory alertMessageFactory;

    private static final Logger log = LoggerFactory.getLogger(ReactiveAbstractAlertManager.class);

    protected ReactiveAbstractAlertManager(AlertMessageFactory alertMessageFactory,
                                           ReactiveAlertMessagePublisher alertMessagePublisher) {
        this.alertMessageFactory = alertMessageFactory;
        this.alertMessagePublisher = alertMessagePublisher;
    }

    @Override
    public Mono<Void> notice(AlertChannel channel, String targetId, Object message) {
        return notice(channel, List.of(AlertTarget.id(targetId)), message);
    }

    @Override
    public Mono<Void> noticeByTag(AlertChannel channel, String tag, Object message) {
        return notice(channel, List.of(AlertTarget.tag(tag)), message);
    }

    @Override
    public Mono<Void> broadcast(AlertChannel channel, Object message) {
        return notice(channel, List.of(AlertTarget.broadcast()), message);
    }

    @Override
    public Mono<Void> notice(AlertChannel channel, List<AlertTarget> targets, Object message) {
        return Mono.fromSupplier(() -> alertMessageFactory.create(
                        channel.namespace(),
                        targets,
                        DefaultAlertMessageType.MESSAGE,
                        message,
                        null
                ))
                .flatMap(alertMessage -> {
                    log.debug("Publishing reactive alert message: {}", alertMessage);
                    return alertMessagePublisher.publish(channel.name(), alertMessage);
                })
                .doOnError(e -> log.error("Failed to publish reactive alert message to targets: {}", targets, e));
    }
}


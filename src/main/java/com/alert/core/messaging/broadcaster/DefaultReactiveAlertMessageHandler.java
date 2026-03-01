package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.FanoutAlertMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DefaultReactiveAlertMessageHandler implements ReactiveAlertMessageHandler {
    private final FanoutAlertMessageSender alertMessageSender;

    private static final Logger log = LoggerFactory.getLogger(DefaultReactiveAlertMessageHandler.class);

    public DefaultReactiveAlertMessageHandler(FanoutAlertMessageSender alertMessageSender) {
        this.alertMessageSender = alertMessageSender;
    }

    @Override
    public Mono<Void> handle(AlertMessage message) {
        String namespace = message.namespace();
        String id = message.id();

        return Mono.fromRunnable(() -> alertMessageSender.send(namespace, id, message))
                .doOnSuccess(v -> log.debug("Alert handled. namespace={}, id={}", namespace, id))
                .doOnError(e -> log.error("Failed to handle alert. namespace={}, id={}", namespace, id, e)).then();
    }
}

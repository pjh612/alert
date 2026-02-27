package com.alert.core.messaging.broadcaster;

import com.alert.cache.ReactiveAlertCacheManager;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultReactiveAlertMessageHandler implements ReactiveAlertMessageHandler {
    private final ReactiveAlertCacheManager alertCacheManager;
    private final AlertMessageSender alertMessageSender;
    private final AlertMessageSupport support;

    public DefaultReactiveAlertMessageHandler(ReactiveAlertCacheManager alertCacheManager,
                                              AlertMessageSender alertMessageSender,
                                              AlertMessageSupport support) {
        this.alertCacheManager = alertCacheManager;
        this.alertMessageSender = alertMessageSender;
        this.support = support;
    }

    @Override
    public Mono<Void> handle(AlertMessage message) {
        String msgId = support.generateMessageId();
        String namespace = message.namespace();

        return Mono.fromRunnable(() -> alertMessageSender.send(namespace, msgId, message))
                .then(Mono.defer(() -> {
                    if (!message.type().isCacheable()) return Mono.empty();

                    return Flux.fromIterable(message.targets())
                            .flatMap(target -> {
                                String key = support.resolveCacheKey(namespace, target);
                                return alertCacheManager.save(key, msgId, message);
                            })
                            .then();
                }));
    }
}

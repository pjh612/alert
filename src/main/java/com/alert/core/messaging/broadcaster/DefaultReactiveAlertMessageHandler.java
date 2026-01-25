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
    private final String topic;

    public DefaultReactiveAlertMessageHandler(ReactiveAlertCacheManager alertCacheManager,
                                              AlertMessageSender alertMessageSender,
                                              AlertMessageSupport support, String topic) {
        this.alertCacheManager = alertCacheManager;
        this.alertMessageSender = alertMessageSender;
        this.support = support;
        this.topic = topic;
    }

    @Override
    public Mono<Void> handle(AlertMessage message) {
        String msgId = support.generateMessageId();

        return Mono.fromRunnable(() -> alertMessageSender.send(msgId, message))
                .then(Mono.defer(() -> {
                    if (!message.type().isCacheable()) return Mono.empty();

                    return Flux.fromIterable(message.targets())
                            .flatMap(target -> {
                                String key = support.resolveCacheKey(topic, target);
                                return alertCacheManager.save(key, msgId, message);
                            })
                            .then();
                }));
    }
}

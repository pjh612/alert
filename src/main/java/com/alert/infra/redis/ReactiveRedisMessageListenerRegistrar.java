package com.alert.infra.redis;

import com.alert.core.messaging.broadcaster.MessageConverter;
import com.alert.core.messaging.broadcaster.ReactiveAlertMessageHandler;
import com.alert.core.messaging.broadcaster.ReactiveAlertMessageListenerRegistrar;
import com.alert.core.messaging.model.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;

public class ReactiveRedisMessageListenerRegistrar implements ReactiveAlertMessageListenerRegistrar<String> {
    private final ReactiveRedisMessageListenerContainer container;

    private static final Logger log = LoggerFactory.getLogger(ReactiveRedisMessageListenerRegistrar.class);

    public ReactiveRedisMessageListenerRegistrar(ReactiveRedisMessageListenerContainer container) {
        this.container = container;
    }

    @Override
    public void register(String topic, ReactiveAlertMessageHandler handler, MessageConverter<String, ? extends AlertMessage> messageConverter) {
        container.receive(ChannelTopic.of(topic))
                .flatMap(it -> {
                    AlertMessage alertMessage = messageConverter.convert(it.getMessage());
                    return handler.handle(alertMessage);
                })
                .doOnError(e -> log.error("Error in reactive redis listener", e))
                .subscribe();
    }
}
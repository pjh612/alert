package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class ReactiveRedisMessageListenerRegistrar implements AlertMessageListenerRegistrar<String> {
    private final ReactiveRedisMessageListenerContainer container;

    public ReactiveRedisMessageListenerRegistrar(ReactiveRedisMessageListenerContainer container) {
        this.container = container;
    }

    @Override
    public void register(String topic, AlertMessageHandler handler, MessageConverter<String, ? extends AlertMessage> messageConverter) {
        container.receive(ChannelTopic.of(topic))
                .doOnNext(it -> {
                    AlertMessage convert = messageConverter.convert(it.getMessage());
                    handler.handle(convert);
                }).subscribe();
    }
}
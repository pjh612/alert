package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

public class RedisMessageListenerRegistrar<T extends AlertMessage> implements MessageListenerRegistrar<T> {
    private final RedisMessageListenerContainer container;

    public RedisMessageListenerRegistrar(RedisMessageListenerContainer container) {
        this.container = container;
    }

    @Override
    public void register(String topic, AlertMessageHandler<T> handler, MessageConverter<byte[], T> messageConverter) {
        MessageListener messageListener = ((message, pattern) -> {
            T alertMessage = messageConverter.convert(message.getBody());
            handler.handle(alertMessage);
        });
        container.addMessageListener(messageListener, ChannelTopic.of(topic));
    }
}

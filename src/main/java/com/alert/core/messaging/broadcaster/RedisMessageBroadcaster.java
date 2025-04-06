package com.alert.core.messaging.broadcaster;

import org.springframework.data.redis.core.RedisTemplate;

public class RedisMessageBroadcaster<T> implements MessageBroadcaster<T> {
    private final RedisTemplate<String, T> redisTemplate;

    public RedisMessageBroadcaster(RedisTemplate<String, T> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void sendMessage(String topic, T message) {
        redisTemplate.convertAndSend(topic, message);
    }
}

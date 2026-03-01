package com.alert.infra.redis;

import com.alert.core.messaging.broadcaster.ReactiveMessageBroadcaster;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Mono;

public class ReactiveRedisMessageBroadcaster<T> implements ReactiveMessageBroadcaster<T, Long> {
    private final ReactiveRedisTemplate<String, T> redisTemplate;

    public ReactiveRedisMessageBroadcaster(ReactiveRedisTemplate<String, T> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public Mono<Long> sendMessage(String topic, T message) {
        return redisTemplate.convertAndSend(topic, message);
    }
}

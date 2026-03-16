package com.alert.infra.redis;

import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.messaging.model.AlertMessage;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;

public class ReactiveRedisAlertCacheManager implements ReactiveAlertCacheManager {
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    private final long cacheTtlSeconds;

    public ReactiveRedisAlertCacheManager(ReactiveRedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper, long cacheTtlSeconds) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.cacheTtlSeconds = cacheTtlSeconds;
    }

    @Override
    public Mono<Boolean> save(String key, String id, AlertMessage value) {
        return redisTemplate.opsForZSet()
                .add(key, objectMapper.writeValueAsString(value), Double.parseDouble(id))
                .flatMap(result -> redisTemplate.expire(key, Duration.ofSeconds(cacheTtlSeconds)).thenReturn(result));
    }

    @Override
    public Mono<Void> saveAll(List<String> keys, String id, AlertMessage value) {
        String json = objectMapper.writeValueAsString(value);
        double score = Double.parseDouble(id);

        return Flux.fromIterable(keys)
                .flatMap(key -> redisTemplate.opsForZSet()
                        .add(key, json, score)
                        .then(redisTemplate.expire(key, Duration.ofSeconds(cacheTtlSeconds))))
                .then();
    }

    @Override
    public Flux<? extends AlertMessage> getFromOffset(String key, String offset, Class<? extends AlertMessage> tClass) {
        long score;
        try {
            score = Long.parseLong(offset);
        } catch (NumberFormatException e) {
            return Flux.error(new IllegalArgumentException("Invalid offset format for Redis ZSet: '" + offset + "'"));
        }
        Range<Double> range = Range.from(Range.Bound.inclusive(score + 1.0))
                .to(Range.Bound.unbounded());
        return redisTemplate.opsForZSet()
                .rangeByScoreWithScores(key, range)
                .flatMap(typedTuple -> Mono.just(objectMapper.readValue(typedTuple.getValue(), tClass)));
    }
}

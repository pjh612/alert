package com.alert.cache;

import com.alert.core.messaging.model.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.util.List;


public class ReactiveRedisAlertCacheManager implements ReactiveAlertCacheManager {
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    private static final Logger log = LoggerFactory.getLogger(ReactiveRedisAlertCacheManager.class);

    public ReactiveRedisAlertCacheManager(ReactiveRedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Boolean> save(String key, String id, AlertMessage value) {
        return redisTemplate.opsForZSet().add(key, objectMapper.writeValueAsString(value), Double.parseDouble(id));
    }

    @Override
    public Flux<? extends AlertMessage> getFromOffset(String key, Long offset, Class<? extends AlertMessage> tClass) {
        Range<Double> range = Range.from(Range.Bound.inclusive(offset + 1.0))
                .to(Range.Bound.unbounded());
        return redisTemplate.opsForZSet()
                .rangeByScoreWithScores(key, range)
                .collectList()
                .flatMapMany(typedTuples -> {
                    if (typedTuples.isEmpty()) {
                        return Flux.empty();
                    }

                    return pop(key, tClass, typedTuples, getMaxOffset(offset, typedTuples));
                })
                .onErrorResume(e -> {
                    log.error("Error fetching and processing range from Redis: {}", e.getMessage(), e);
                    return Flux.empty();
                });
    }

    private Flux<? extends AlertMessage> pop(String key, Class<? extends AlertMessage> tClass, List<ZSetOperations.TypedTuple<String>> typedTuples, double maxOffset) {
        return redisTemplate.opsForZSet()
                .removeRangeByScore(key, Range.of(Range.Bound.inclusive(0.0), Range.Bound.inclusive(maxOffset)))
                .thenMany(Flux.fromIterable(typedTuples))
                .flatMap(typedTuple -> Mono.just(objectMapper.readValue(typedTuple.getValue(), tClass)));
    }

    private double getMaxOffset(Long offset, List<ZSetOperations.TypedTuple<String>> typedTuples) {
        return typedTuples.stream()
                .mapToDouble(ZSetOperations.TypedTuple::getScore)
                .max()
                .orElse(offset);
    }


}

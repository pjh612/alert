package com.alert.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
public class RedisAlertCacheManager<T> implements AlertCacheManager<T> {
    private final RedisTemplate<String, String> messageCache;
    private final ObjectMapper objectMapper;

    public RedisAlertCacheManager(RedisTemplate<String, String> messageCache, ObjectMapper objectMapper) {
        this.messageCache = messageCache;
        this.objectMapper = objectMapper;
    }

    @Override
    public Boolean save(String key, String id, T value) {
        try {
            return messageCache.opsForZSet().add(key, objectMapper.writeValueAsString(value), Double.parseDouble(id));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<T> getFromOffset(String key, Long offset, Class<T> tClass) {
        Set<ZSetOperations.TypedTuple<String>> typedTuples = messageCache.opsForZSet().rangeByScoreWithScores(key, offset + 1, Double.MAX_VALUE);
        double maxOffset = typedTuples.stream()
                .mapToDouble(ZSetOperations.TypedTuple::getScore)
                .max()
                .orElse(offset);
        messageCache.opsForZSet().removeRangeByScore(key, 0, maxOffset);

        return typedTuples
                .stream()
                .map(it -> {
                    try {
                        return objectMapper.readValue(it.getValue(), tClass);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();

    }
}


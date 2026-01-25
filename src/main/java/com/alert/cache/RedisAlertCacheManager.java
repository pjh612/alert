package com.alert.cache;

import com.alert.core.messaging.model.AlertMessage;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Set;

@Component
public class RedisAlertCacheManager implements AlertCacheManager {
    private final RedisTemplate<String, String> messageCache;
    private final ObjectMapper objectMapper;

    public RedisAlertCacheManager(RedisTemplate<String, String> messageCache, ObjectMapper objectMapper) {
        this.messageCache = messageCache;
        this.objectMapper = objectMapper;
    }

    @Override
    public Boolean save(String key, String id, AlertMessage value) {
        return messageCache.opsForZSet().add(key, objectMapper.writeValueAsString(value), Double.parseDouble(id));
    }

    @Override
    public List<? extends AlertMessage> getFromOffset(String key, Long offset, Class<? extends AlertMessage> tClass) {
        Set<ZSetOperations.TypedTuple<String>> typedTuples = messageCache.opsForZSet().rangeByScoreWithScores(key, offset + 1, Double.MAX_VALUE);
        double maxOffset = typedTuples.stream()
                .mapToDouble(ZSetOperations.TypedTuple::getScore)
                .max()
                .orElse(offset);
        messageCache.opsForZSet().removeRangeByScore(key, 0, maxOffset);

        return typedTuples
                .stream()
                .map(it -> objectMapper.readValue(it.getValue(), tClass))
                .toList();

    }
}


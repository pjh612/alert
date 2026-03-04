package com.alert.infra.redis;

import com.alert.core.cache.AlertCacheManager;
import com.alert.core.messaging.model.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;
import java.util.Set;

public class RedisAlertCacheManager implements AlertCacheManager {
    private final RedisTemplate<String, String> messageCache;
    private final ObjectMapper objectMapper;
    private final long cacheTtlSeconds;

    private static final Logger log = LoggerFactory.getLogger(RedisAlertCacheManager.class);

    public RedisAlertCacheManager(RedisTemplate<String, String> messageCache, ObjectMapper objectMapper, long cacheTtlSeconds) {
        this.messageCache = messageCache;
        this.objectMapper = objectMapper;
        this.cacheTtlSeconds = cacheTtlSeconds;
    }

    @Override
    public Boolean save(String key, String id, AlertMessage value) {
        Boolean result = messageCache.opsForZSet().add(key, objectMapper.writeValueAsString(value), Double.parseDouble(id));
        messageCache.expire(key, Duration.ofSeconds(cacheTtlSeconds));

        if (log.isDebugEnabled()) {
            log.debug("Saved alert message with id {} and value {}", id, value);
        }
        return result;
    }

    @Override
    public List<? extends AlertMessage> getFromOffset(String key, String offset, Class<? extends AlertMessage> tClass) {
        long score;
        try {
            score = Long.parseLong(offset);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid offset format for Redis ZSet: '" + offset + "'");
        }
        Set<ZSetOperations.TypedTuple<String>> typedTuples =
                messageCache.opsForZSet().rangeByScoreWithScores(key, score + 1, Double.MAX_VALUE);

        return typedTuples.stream()
                .map(it -> objectMapper.readValue(it.getValue(), tClass))
                .toList();
    }
}

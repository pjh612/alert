package com.alert.infra.redis;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import org.springframework.data.redis.core.DefaultTypedTuple;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveRedisAlertCacheManagerTest {

    @Mock
    ReactiveRedisTemplate<String, String> redisTemplate;
    @Mock
    ReactiveZSetOperations<String, String> zSetOps;

    ObjectMapper objectMapper;
    ReactiveRedisAlertCacheManager cacheManager;

    private static final long CACHE_TTL_SECONDS = 1800L;
    private static final String CACHE_KEY = "alert:test:user:user1";
    private static final String MSG_ID = "1700000000001";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        cacheManager = new ReactiveRedisAlertCacheManager(redisTemplate, objectMapper, CACHE_TTL_SECONDS);
    }

    private AlertMessage createMessage(String id) {
        return new DefaultAlertMessage(
                id, "test", List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE, "body", false, null);
    }

    @Test
    @DisplayName("save: ZADD 후 EXPIRE 호출")
    void save_addsToZSetAndSetsExpire() {
        AlertMessage msg = createMessage(MSG_ID);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOps);
        when(zSetOps.add(eq(CACHE_KEY), anyString(), eq(Double.parseDouble(MSG_ID)))).thenReturn(Mono.just(true));
        when(redisTemplate.expire(eq(CACHE_KEY), any())).thenReturn(Mono.just(true));

        StepVerifier.create(cacheManager.save(CACHE_KEY, MSG_ID, msg))
                .expectNext(true)
                .verifyComplete();

        verify(zSetOps).add(eq(CACHE_KEY), anyString(), eq(Double.parseDouble(MSG_ID)));
        verify(redisTemplate).expire(eq(CACHE_KEY), any());
    }

    @Test
    @DisplayName("saveAll: JSON 직렬화 1회로 여러 키에 ZADD + EXPIRE 호출")
    void saveAll_serializesOnceAndWritesToAllKeys() {
        AlertMessage msg = createMessage(MSG_ID);
        List<String> keys = List.of("key1", "key2", "key3");
        double expectedScore = Double.parseDouble(MSG_ID);

        when(redisTemplate.opsForZSet()).thenReturn(zSetOps);
        when(zSetOps.add(anyString(), anyString(), eq(expectedScore))).thenReturn(Mono.just(true));
        when(redisTemplate.expire(anyString(), any())).thenReturn(Mono.just(true));

        StepVerifier.create(cacheManager.saveAll(keys, MSG_ID, msg))
                .verifyComplete();

        verify(zSetOps, times(3)).add(anyString(), anyString(), eq(expectedScore));
        verify(redisTemplate, times(3)).expire(anyString(), any());
    }

    @Test
    @DisplayName("saveAll: 모든 키에 동일한 JSON 값이 저장된다")
    void saveAll_writesIdenticalJsonToAllKeys() {
        AlertMessage msg = createMessage(MSG_ID);
        List<String> keys = List.of("key1", "key2");

        when(redisTemplate.opsForZSet()).thenReturn(zSetOps);
        when(zSetOps.add(anyString(), anyString(), anyDouble())).thenReturn(Mono.just(true));
        when(redisTemplate.expire(anyString(), any())).thenReturn(Mono.just(true));

        org.mockito.ArgumentCaptor<String> jsonCaptor = org.mockito.ArgumentCaptor.forClass(String.class);

        StepVerifier.create(cacheManager.saveAll(keys, MSG_ID, msg))
                .verifyComplete();

        verify(zSetOps, times(2)).add(anyString(), jsonCaptor.capture(), anyDouble());
        List<String> capturedJsons = jsonCaptor.getAllValues();
        org.assertj.core.api.Assertions.assertThat(capturedJsons.get(0)).isEqualTo(capturedJsons.get(1));
    }

    @Test
    @DisplayName("getFromOffset: offset 이후 메시지 조회")
    void getFromOffset_returnsMessagesAfterOffset() throws Exception {
        AlertMessage msg = createMessage(MSG_ID);
        String json = objectMapper.writeValueAsString(msg);

        when(redisTemplate.opsForZSet()).thenReturn(zSetOps);

        Set<org.springframework.data.redis.core.DefaultTypedTuple<String>> typedTuples = new HashSet<>();
        typedTuples.add(new DefaultTypedTuple<>(json, Double.parseDouble(MSG_ID)));
        when(zSetOps.rangeByScoreWithScores(eq(CACHE_KEY), any(Range.class))).thenReturn(Flux.fromIterable(typedTuples));

        StepVerifier.create(cacheManager.getFromOffset(CACHE_KEY, "1700000000000", DefaultAlertMessage.class))
                .expectNextMatches(m -> m.id().equals(MSG_ID))
                .verifyComplete();
    }
}

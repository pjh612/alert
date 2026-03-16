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
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.DefaultTypedTuple;

import org.mockito.ArgumentCaptor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisAlertCacheManagerTest {

    @Mock
    RedisTemplate<String, String> messageCache;
    @Mock
    ZSetOperations<String, String> zSetOps;

    ObjectMapper objectMapper;
    RedisAlertCacheManager cacheManager;

    private static final long CACHE_TTL_SECONDS = 1800L;
    private static final String CACHE_KEY = "alert:test:user:user1";
    private static final String MSG_ID = "1700000000001";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        cacheManager = new RedisAlertCacheManager(messageCache, objectMapper, CACHE_TTL_SECONDS);
        when(messageCache.opsForZSet()).thenReturn(zSetOps);
    }

    private AlertMessage createMessage(String id) {
        return new DefaultAlertMessage(
                id, "test", List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE, "body", false, null);
    }

    @Test
    @DisplayName("save: ZADD 후 EXPIRE 호출")
    void save_addsToZSetAndSetsExpire() throws Exception {
        AlertMessage msg = createMessage(MSG_ID);
        when(zSetOps.add(eq(CACHE_KEY), anyString(), eq(Double.parseDouble(MSG_ID)))).thenReturn(true);

        Boolean result = cacheManager.save(CACHE_KEY, MSG_ID, msg);

        assertThat(result).isTrue();
        verify(zSetOps).add(eq(CACHE_KEY), anyString(), eq(Double.parseDouble(MSG_ID)));
        verify(messageCache).expire(eq(CACHE_KEY), any());
    }

    @Test
    @DisplayName("saveAll: JSON 직렬화 1회로 여러 키에 ZADD + EXPIRE 호출")
    void saveAll_serializesOnceAndWritesToAllKeys() {
        AlertMessage msg = createMessage(MSG_ID);
        List<String> keys = List.of("key1", "key2", "key3");
        double expectedScore = Double.parseDouble(MSG_ID);

        cacheManager.saveAll(keys, MSG_ID, msg);

        verify(zSetOps, times(3)).add(anyString(), anyString(), eq(expectedScore));
        verify(messageCache, times(3)).expire(anyString(), any());

        for (String key : keys) {
            verify(zSetOps).add(eq(key), anyString(), eq(expectedScore));
            verify(messageCache).expire(eq(key), any());
        }
    }

    @Test
    @DisplayName("saveAll: 모든 키에 동일한 JSON 값이 저장된다")
    void saveAll_writesIdenticalJsonToAllKeys() {
        AlertMessage msg = createMessage(MSG_ID);
        List<String> keys = List.of("key1", "key2");

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);

        cacheManager.saveAll(keys, MSG_ID, msg);

        verify(zSetOps, times(2)).add(anyString(), jsonCaptor.capture(), anyDouble());
        List<String> capturedJsons = jsonCaptor.getAllValues();
        assertThat(capturedJsons.get(0)).isEqualTo(capturedJsons.get(1));
    }

    @Test
    @DisplayName("getFromOffset: offset 이후 메시지 조회")
    void getFromOffset_returnsMessagesAfterOffset() throws Exception {
        AlertMessage msg = createMessage(MSG_ID);
        String json = objectMapper.writeValueAsString(msg);

        Set<ZSetOperations.TypedTuple<String>> typedTuples = new HashSet<>();
        DefaultTypedTuple<String> tuple = mock(DefaultTypedTuple.class);
        when(tuple.getValue()).thenReturn(json);
        typedTuples.add(tuple);
        when(zSetOps.rangeByScoreWithScores(eq(CACHE_KEY), anyDouble(), eq(Double.MAX_VALUE)))
                .thenReturn(typedTuples);

        List<? extends AlertMessage> result = cacheManager.getFromOffset(CACHE_KEY, "1700000000001", DefaultAlertMessage.class);

        assertThat(result).hasSize(1);
    }
}

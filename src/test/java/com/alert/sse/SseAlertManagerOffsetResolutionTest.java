package com.alert.sse;

import com.alert.core.cache.AlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.*;
import com.alert.core.messaging.publisher.AlertMessagePublisher;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SseAlertManagerOffsetResolutionTest {

    @Mock AlertMessagePublisher alertMessagePublisher;
    @Mock AlertMessageFactory alertMessageFactory;
    @Mock TagBasedAlertSessionRepository<SseEmitter> emitterRepository;
    @Mock AlertCacheManager alertCacheManager;
    @Mock AlertMessageSupport support;

    SseAlertManager manager;

    private static final String NAMESPACE = "test-ns";
    private static final String SUBSCRIBER_ID = "user1";
    private static final String CACHE_KEY = "alert:test-ns:user:user1";

    private AlertChannel channel;

    @BeforeEach
    void setUp() {
        manager = new SseAlertManager(
                alertMessagePublisher, alertMessageFactory,
                emitterRepository, alertCacheManager, support, DefaultAlertMessage.class);

        channel = () -> NAMESPACE;

        when(support.resolveCacheKey(eq(NAMESPACE), any(AlertTarget.class))).thenReturn(CACHE_KEY);
        when(support.generateMessageId()).thenReturn("100");
        doReturn(Collections.emptyList()).when(alertCacheManager).getFromOffset(any(), any(), any());
    }

    @Test
    @DisplayName("lastEventId 없으면 재전송 안함")
    void noLastEventId_doesNotRepublish() {
        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

        verify(alertCacheManager, never()).getFromOffset(any(), any(), any());
    }

    @Test
    @DisplayName("lastEventId=100이면 offset 100으로 재전송")
    void withLastEventId100_republishWithOffset100() {
        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), "100", 30000L);

        verify(alertCacheManager).getFromOffset(CACHE_KEY, 100L, DefaultAlertMessage.class);
    }

    @Test
    @DisplayName("태그를 포함한 재전송: ID 및 각 태그 캐시 키로 getFromOffset 호출")
    void withTags_getFromOffsetCalledForIdAndEachTag() {
        String tagKey1 = "alert:test-ns:tag:vip";
        String tagKey2 = "alert:test-ns:tag:admin";
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.id(SUBSCRIBER_ID))).thenReturn(CACHE_KEY);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.tag("vip"))).thenReturn(tagKey1);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.tag("admin"))).thenReturn(tagKey2);
        doReturn(Collections.emptyList()).when(alertCacheManager).getFromOffset(any(), any(), any());

        manager.subscribe(channel, SUBSCRIBER_ID, List.of("vip", "admin"), "100", 30000L);

        verify(alertCacheManager).getFromOffset(CACHE_KEY, 100L, DefaultAlertMessage.class);
        verify(alertCacheManager).getFromOffset(tagKey1, 100L, DefaultAlertMessage.class);
        verify(alertCacheManager).getFromOffset(tagKey2, 100L, DefaultAlertMessage.class);
    }

    @Test
    @DisplayName("재전송 메시지는 Kafka를 거치지 않고 emitter로 직접 전송된다")
    void missedMessages_sentDirectlyToEmitter_notViaKafka() {
        AlertMessage missed = new DefaultAlertMessage(
                "103", NAMESPACE, List.of(AlertTarget.id(SUBSCRIBER_ID)),
                DefaultAlertMessageType.MESSAGE, "missed body", false, null);
        AlertMessage connectMsg = mock(AlertMessage.class);
        when(alertMessageFactory.onConnect(any(), any(), any())).thenReturn(connectMsg);
        doReturn(List.of(missed)).when(alertCacheManager)
                .getFromOffset(CACHE_KEY, 100L, DefaultAlertMessage.class);

        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), "100", 30000L);

        // connect 메시지만 Kafka로 발행, replay 메시지는 emitter로 직접 전송
        verify(alertMessagePublisher, times(1)).publish(any(), eq(connectMsg));
        verify(alertMessageFactory, never()).onReplay(any(), any(), any(), any());
    }
}

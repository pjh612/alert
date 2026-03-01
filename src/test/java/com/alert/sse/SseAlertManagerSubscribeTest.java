package com.alert.sse;

import com.alert.core.cache.AlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.*;
import com.alert.core.messaging.publisher.AlertMessagePublisher;
import com.alert.core.session.AlertSession;
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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SseAlertManagerSubscribeTest {

    @Mock
    AlertMessagePublisher alertMessagePublisher;
    @Mock
    AlertMessageFactory alertMessageFactory;
    @Mock
    TagBasedAlertSessionRepository<SseEmitter> emitterRepository;
    @Mock
    AlertCacheManager alertCacheManager;
    @Mock
    AlertMessageSupport support;

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
    @DisplayName("subscribe: 성공 시 SseEmitter 반환")
    void subscribe_returnsSseEmitter() {
        SseEmitter emitter = manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

        assertThat(emitter).isNotNull();
    }

    @Test
    @DisplayName("subscribe: 연결 메시지 전송")
    void subscribe_sendsConnectMessage() {
        AlertMessage connectMsg = mock(AlertMessage.class);
        when(alertMessageFactory.onConnect(eq(NAMESPACE), eq(SUBSCRIBER_ID), any())).thenReturn(connectMsg);

        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

        verify(alertMessagePublisher).publish(NAMESPACE, connectMsg);
    }

    @Test
    @DisplayName("subscribe: emitter 레포지토리에 저장")
    void subscribe_storesEmitterInRepository() {
        manager.subscribe(channel, SUBSCRIBER_ID, List.of("vip"), null, 30000L);

        verify(emitterRepository).put(eq(NAMESPACE), eq(SUBSCRIBER_ID), eq(new java.util.HashSet<>(List.of("vip"))), any(SseEmitter.class));
    }

    @Test
    @DisplayName("subscribe: invalid lastEventId는 재전송 안함")
    void subscribe_invalidLastEventId_doesNotRepublish() {
        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), "invalid", 30000L);

        verify(alertCacheManager, never()).getFromOffset(any(), any(), any());
    }

    @Test
    @DisplayName("subscribe: 중복 메시지는 머지되어 emitter로 한 번만 전송되고 Kafka는 타지 않는다")
    void subscribe_duplicateMessages_mergedById() {
        String duplicateId = "100";
        AlertMessage msg1 = new DefaultAlertMessage(
                duplicateId, NAMESPACE, List.of(AlertTarget.id(SUBSCRIBER_ID)),
                DefaultAlertMessageType.MESSAGE, "body1", false, null);
        AlertMessage msg2 = new DefaultAlertMessage(
                duplicateId, NAMESPACE, List.of(AlertTarget.id(SUBSCRIBER_ID)),
                DefaultAlertMessageType.MESSAGE, "body2", false, null);
        AlertMessage connectMsg = mock(AlertMessage.class);
        when(alertMessageFactory.onConnect(any(), any(), any())).thenReturn(connectMsg);
        doReturn(List.of(msg1, msg2)).when(alertCacheManager).getFromOffset(eq(CACHE_KEY), eq(50L), any());

        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), "50", 30000L);

        // 중복된 ID의 메시지는 하나로 머지, Kafka publish는 connect 1회만
        verify(alertMessagePublisher, times(1)).publish(any(), eq(connectMsg));
        verify(alertMessageFactory, never()).onReplay(any(), any(), any(), any());
    }
}

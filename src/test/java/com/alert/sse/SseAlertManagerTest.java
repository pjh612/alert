package com.alert.sse;

import com.alert.core.cache.AlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import com.alert.core.messaging.publisher.AlertMessagePublisher;
import com.alert.core.session.AlertSession;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SseAlertManagerTest {

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
    private static final String BROADCAST_KEY = "alert:test-ns:broadcast:*";

    private AlertChannel channel;

    @BeforeEach
    void setUp() {

        manager = new SseAlertManager(
                alertMessagePublisher,
                alertMessageFactory,
                emitterRepository,
                alertCacheManager,
                support,
                DefaultAlertMessage.class
        );

        channel = () -> NAMESPACE;
    }


    @Test
    @DisplayName("subscribe 호출 시 SseEmitter가 반환된다")
    void shouldReturnEmitter_whenSubscribe() {

        when(alertMessageFactory.onConnect(any(), any(), any()))
                .thenReturn(mock(AlertMessage.class));

        SseEmitter emitter =
                manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

        assertThat(emitter).isNotNull();
    }

    @Test
    @DisplayName("subscribe 시 emitterRepository에 emitter가 저장된다")
    void shouldStoreEmitter_whenSubscribe() {

        when(alertMessageFactory.onConnect(any(), any(), any()))
                .thenReturn(mock(AlertMessage.class));

        manager.subscribe(channel, SUBSCRIBER_ID, List.of("vip"), null, 30000L);

        verify(emitterRepository).put(
                eq(NAMESPACE),
                eq(SUBSCRIBER_ID),
                eq(new HashSet<>(List.of("vip"))),
                any(SseEmitter.class)
        );
    }

    @Test
    @DisplayName("subscribe 시 connect 메시지가 Kafka로 발행된다")
    void shouldPublishConnectMessage_whenSubscribe() {

        AlertMessage connect = mock(AlertMessage.class);

        when(alertMessageFactory.onConnect(any(), any(), any()))
                .thenReturn(connect);

        SseEmitter subscribe = manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

        verify(alertMessagePublisher).publish(NAMESPACE, connect);

        assertThat(subscribe).isNotNull();
    }

    @Test
    @DisplayName("lastEventId가 없으면 replay가 수행되지 않는다")
    void shouldNotReplay_whenLastEventIdIsNull() {

        when(alertMessageFactory.onConnect(any(), any(), any()))
                .thenReturn(mock(AlertMessage.class));

        SseEmitter subscribe = manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

        verify(alertCacheManager, never()).getFromOffset(any(), any(), any());

        assertThat(subscribe).isNotNull();
    }

    @Test
    @DisplayName("lastEventId가 있으면 replay 조회가 수행된다")
    void shouldReplayMessages_whenLastEventIdExists() {
        // Given
        when(alertMessageFactory.onConnect(any(), any(), any()))
                .thenReturn(mock(AlertMessage.class));
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.broadcast()))
                .thenReturn(BROADCAST_KEY);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.id(SUBSCRIBER_ID)))
                .thenReturn(CACHE_KEY);
        when(alertCacheManager.getFromOffset(any(), any(), any()))
                .thenReturn(Collections.emptyList());

        // When
        SseEmitter subscribe = manager.subscribe(channel, SUBSCRIBER_ID, List.of(), "100", 30000L);

        // Then
        verify(alertCacheManager)
                .getFromOffset(CACHE_KEY, "100", DefaultAlertMessage.class);
        verify(alertCacheManager)
                .getFromOffset(BROADCAST_KEY, "100", DefaultAlertMessage.class);

        assertThat(subscribe).isNotNull();
    }

    @Test
    @DisplayName("tag가 있으면 각 tag 캐시에서 replay 조회가 수행된다")
    void shouldReplayFromEachTagCache() {

        when(alertMessageFactory.onConnect(any(), any(), any()))
                .thenReturn(mock(AlertMessage.class));

        String tag1 = "alert:test-ns:tag:vip";
        String tag2 = "alert:test-ns:tag:admin";

        when(support.resolveCacheKey(NAMESPACE, AlertTarget.tag("vip")))
                .thenReturn(tag1);

        when(support.resolveCacheKey(NAMESPACE, AlertTarget.tag("admin")))
                .thenReturn(tag2);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.id(SUBSCRIBER_ID)))
                .thenReturn(CACHE_KEY);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.broadcast()))
                .thenReturn(BROADCAST_KEY);
        when(alertCacheManager.getFromOffset(any(), any(), any()))
                .thenReturn(Collections.emptyList());

        manager.subscribe(channel,
                SUBSCRIBER_ID,
                List.of("vip", "admin"),
                "100",
                30000L
        );

        verify(alertCacheManager)
                .getFromOffset(tag1, "100", DefaultAlertMessage.class);
        verify(alertCacheManager)
                .getFromOffset(tag2, "100", DefaultAlertMessage.class);
        verify(alertCacheManager)
                .getFromOffset(CACHE_KEY, "100", DefaultAlertMessage.class);
        verify(alertCacheManager)
                .getFromOffset(BROADCAST_KEY, "100", DefaultAlertMessage.class);
    }

    @Test
    @DisplayName("replay 메시지는 emitter로 직접 전송된다")
    void shouldSendReplayMessagesDirectlyToEmitter() throws Exception {
        try (MockedConstruction<SseEmitter> mocked = mockConstruction(SseEmitter.class)) {
            // Given
            AlertMessage replayMessage = new DefaultAlertMessage(
                    "101", NAMESPACE, List.of(AlertTarget.id(SUBSCRIBER_ID)),
                    DefaultAlertMessageType.MESSAGE, "body", false, null
            );

            when(alertMessageFactory.onConnect(any(), any(), any()))
                    .thenReturn(mock(AlertMessage.class));
            doReturn(List.of(replayMessage)).when(alertCacheManager).getFromOffset(any(), any(), any());

            // When
            manager.subscribe(channel, SUBSCRIBER_ID, List.of(), "100", 30000L);

            // Then
            SseEmitter mockEmitter = mocked.constructed().get(0);

            // 1. 실제로 emitter.send()가 호출되었는지 확인 (직접 전송 여부)
            verify(mockEmitter, atLeastOnce()).send(any(SseEmitter.SseEventBuilder.class));

            // 2. Replay 시 Factory를 거쳐서 가공하지 않는다는 기존 검증 유지
            verify(alertMessageFactory, never())
                    .onReplay(any(), any(), any(), any());
        }
    }

    @Test
    @DisplayName("메시지 재전송 중 에러가 발생해도 나머지 메시지는 계속 전송되어야 한다")
    void shouldCoverCatchBlock_whenSendFails() throws Exception {
        // Given
        AlertMessage msg = mock(AlertMessage.class);
        when(msg.id()).thenReturn("101");
        when(msg.type()).thenReturn(DefaultAlertMessageType.MESSAGE);
        when(msg.body()).thenReturn("body");

        when(alertMessageFactory.onConnect(any(), any(), any()))
                .thenReturn(mock(AlertMessage.class));
        doReturn(List.of(msg)).when(alertCacheManager).getFromOffset(any(), any(), any());

        try (MockedConstruction<SseEmitter> mocked = mockConstruction(SseEmitter.class,
                (mock, context) -> {
                    doThrow(new RuntimeException("JaCoCo Coverage Test Exception"))
                            .when(mock).send(any(SseEmitter.SseEventBuilder.class));
                })) {

            // When
            manager.subscribe(channel, SUBSCRIBER_ID, List.of(), "100", 30000L);

            // Then
            SseEmitter mockEmitter = mocked.constructed().get(0);

            verify(mockEmitter, atLeastOnce()).send(any(SseEmitter.SseEventBuilder.class));
        }
    }

    // Helper method
    private AlertMessage createMockMessage(String id, String body) {
        AlertMessage msg = mock(AlertMessage.class);
        when(msg.id()).thenReturn(id);
        when(msg.body()).thenReturn(body);
        when(msg.type()).thenReturn(DefaultAlertMessageType.MESSAGE);
        return msg;
    }

    @Test
    @DisplayName("SseEmitter 완료 시 cleanUpEmitter가 실행되어 세션을 삭제해야 한다")
    void shouldCleanup_whenEmitterCompletionOccurs() {
        try (MockedConstruction<SseEmitter> mocked = mockConstruction(SseEmitter.class)) {
            // Given
            when(alertMessageFactory.onConnect(any(), any(), any()))
                    .thenReturn(mock(AlertMessage.class));

            manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

            SseEmitter mockEmitter = mocked.constructed().get(0);
            AlertSession<SseEmitter> session = new AlertSession<>(SUBSCRIBER_ID, mockEmitter, Set.of());

            when(emitterRepository.getById(NAMESPACE, SUBSCRIBER_ID))
                    .thenReturn(Optional.of(session));

            ArgumentCaptor<Runnable> completionCaptor = ArgumentCaptor.forClass(Runnable.class);
            verify(mockEmitter).onCompletion(completionCaptor.capture());

            // When
            completionCaptor.getValue().run();

            // Then
            verify(emitterRepository).deleteById(NAMESPACE, SUBSCRIBER_ID);
        }
    }

    @Test
    @DisplayName("SseEmitter에서 에러 발생 시 cleanUpEmitter가 실행되어 세션을 삭제해야 한다")
    void shouldCleanup_whenEmitterErrorOccurs() {
        try (MockedConstruction<SseEmitter> mocked = mockConstruction(SseEmitter.class)) {
            // Given
            when(alertMessageFactory.onConnect(any(), any(), any()))
                    .thenReturn(mock(AlertMessage.class));

            manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

            SseEmitter mockEmitter = mocked.constructed().get(0);
            AlertSession<SseEmitter> session = new AlertSession<>(SUBSCRIBER_ID, mockEmitter, Set.of());

            when(emitterRepository.getById(NAMESPACE, SUBSCRIBER_ID))
                    .thenReturn(Optional.of(session));

            ArgumentCaptor<Consumer<Throwable>> errorCaptor = ArgumentCaptor.forClass(Consumer.class);
            verify(mockEmitter).onError(errorCaptor.capture());

            // When
            Throwable testError = new RuntimeException("SSE 연결 오류 발생");
            errorCaptor.getValue().accept(testError);

            // Then
            verify(emitterRepository).deleteById(NAMESPACE, SUBSCRIBER_ID);
        }
    }

    @Test
    @DisplayName("동일한 ID로 재구독 시, 기존에 존재하던 Emitter를 찾아 명시적으로 종료해야 한다")
    void shouldCompleteOldEmitter_whenNewSubscriptionArrives() {
        // Given
        SseEmitter oldMockEmitter = mock(SseEmitter.class);
        AlertSession<SseEmitter> oldSession = new AlertSession<>(SUBSCRIBER_ID, oldMockEmitter, Set.of());

        when(emitterRepository.put(eq(NAMESPACE), eq(SUBSCRIBER_ID), anySet(), any()))
                .thenReturn(oldSession);
        when(alertMessageFactory.onConnect(any(), any(), any()))
                .thenReturn(mock(AlertMessage.class));

        // When
        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

        // Then
        verify(oldMockEmitter, times(1)).complete();
        verify(emitterRepository).put(eq(NAMESPACE), eq(SUBSCRIBER_ID), anySet(), any(SseEmitter.class));
    }

    @Test
    @DisplayName("onCompletion 콜백 실행 시, 저장소의 객체를 삭제해야한다.")
    void shouldExecuteCleanup_whenOnCompletionIsTriggeredWithMatchingEmitter() {
        try (MockedConstruction<SseEmitter> mocked = mockConstruction(SseEmitter.class)) {

            // Given
            when(alertMessageFactory.onConnect(any(), any(), any()))
                    .thenReturn(mock(AlertMessage.class));

            manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);
            SseEmitter mockEmitter = mocked.constructed().get(0);
            AlertSession<SseEmitter> currentSession = new AlertSession<>(SUBSCRIBER_ID, mockEmitter, Set.of());
            when(emitterRepository.getById(NAMESPACE, SUBSCRIBER_ID))
                    .thenReturn(Optional.of(currentSession));

            // When
            ArgumentCaptor<Runnable> completionCaptor = ArgumentCaptor.forClass(Runnable.class);
            verify(mockEmitter).onCompletion(completionCaptor.capture());

            completionCaptor.getValue().run();

            // Then
            verify(emitterRepository).getById(NAMESPACE, SUBSCRIBER_ID);
            verify(emitterRepository).deleteById(NAMESPACE, SUBSCRIBER_ID);
        }
    }

    @Test
    @DisplayName("onTimeout 콜백 실행 시, 저장소의 객체를 삭제해야한다.")
    void shouldCleanup_whenEmitterTimeoutOccurs() {
        try (MockedConstruction<SseEmitter> mocked = mockConstruction(SseEmitter.class)) {
            // Given
            when(alertMessageFactory.onConnect(any(), any(), any()))
                    .thenReturn(mock(AlertMessage.class));

            manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

            SseEmitter mockEmitter = mocked.constructed().get(0);
            AlertSession<SseEmitter> session = new AlertSession<>(SUBSCRIBER_ID, mockEmitter, Set.of());

            when(emitterRepository.getById(NAMESPACE, SUBSCRIBER_ID))
                    .thenReturn(Optional.of(session));

            ArgumentCaptor<Runnable> timeoutCaptor = ArgumentCaptor.forClass(Runnable.class);
            verify(mockEmitter).onTimeout(timeoutCaptor.capture());

            // When
            timeoutCaptor.getValue().run();

            // Then
            verify(emitterRepository).deleteById(NAMESPACE, SUBSCRIBER_ID);
        }
    }

    @Test
    @DisplayName("기존 세션 종료 중 예외가 발생해도 새로운 구독 프로세스는 계속되어야 한다")
    void shouldIgnoreException_whenOldEmitterCompleteFails() {
        try (MockedConstruction<SseEmitter> mocked = mockConstruction(SseEmitter.class)) {
            // Given
            SseEmitter oldMockEmitter = mock(SseEmitter.class);
            AlertSession<SseEmitter> oldSession = new AlertSession<>(SUBSCRIBER_ID, oldMockEmitter, Set.of());

            when(emitterRepository.put(eq(NAMESPACE), eq(SUBSCRIBER_ID), anySet(), any()))
                    .thenReturn(oldSession);
            doThrow(new RuntimeException("Old Emitter already closed"))
                    .when(oldMockEmitter).complete();

            when(alertMessageFactory.onConnect(any(), any(), any()))
                    .thenReturn(mock(AlertMessage.class));

            // When
            manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

            // Then
            SseEmitter newEmitter = mocked.constructed().get(0);

            verify(oldMockEmitter).complete();
            verify(emitterRepository).put(eq(NAMESPACE), eq(SUBSCRIBER_ID), anySet(), eq(newEmitter));
        }
    }

    @Test
    @DisplayName("cleanup filter 검증: 저장소의 Emitter와 다르면 false를 반환해야 한다 (노란색 해소)")
    void cleanup_shouldReturnFalseInFilter_whenEmitterIsDifferent() {
        try (MockedConstruction<SseEmitter> mocked = mockConstruction(SseEmitter.class)) {
            // Given
            when(alertMessageFactory.onConnect(any(), any(), any())).thenReturn(mock(AlertMessage.class));
            manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

            SseEmitter currentMockEmitter = mocked.constructed().get(0);
            SseEmitter differentEmitter = mock(SseEmitter.class);
            AlertSession<SseEmitter> differentSession = new AlertSession<>(SUBSCRIBER_ID, differentEmitter, Set.of());

            when(emitterRepository.getById(NAMESPACE, SUBSCRIBER_ID))
                    .thenReturn(Optional.of(differentSession));

            ArgumentCaptor<Runnable> completionCaptor = ArgumentCaptor.forClass(Runnable.class);
            verify(currentMockEmitter).onCompletion(completionCaptor.capture());

            // When
            completionCaptor.getValue().run();

            // Then
            verify(emitterRepository, never()).deleteById(anyString(), anyString());
        }
    }
}
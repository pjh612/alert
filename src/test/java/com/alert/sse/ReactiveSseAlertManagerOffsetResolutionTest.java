package com.alert.sse;

import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.*;
import com.alert.core.messaging.publisher.ReactiveAlertMessagePublisher;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveSseAlertManagerOffsetResolutionTest {

    @Mock ReactiveAlertMessagePublisher alertMessagePublisher;
    @Mock AlertMessageFactory alertMessageFactory;
    @Mock TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> repository;
    @Mock ReactiveAlertCacheManager cacheManager;
    @Mock AlertMessageSupport support;

    ReactiveSseAlertManager manager;

    private static final String NAMESPACE = "test-ns";
    private static final String SUBSCRIBER_ID = "user1";
    private static final String CACHE_KEY = "alert:test-ns:user:user1";
    private static final String BROADCAST_KEY = "alert:test-ns:broadcast:*";

    private AlertChannel channel;

    @BeforeEach
    void setUp() {
        manager = new ReactiveSseAlertManager(
                alertMessagePublisher, alertMessageFactory,
                repository, cacheManager, support, DefaultAlertMessage.class);

        channel = () -> NAMESPACE;

        when(support.resolveCacheKey(eq(NAMESPACE), any(AlertTarget.class))).thenReturn(CACHE_KEY);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.broadcast())).thenReturn(BROADCAST_KEY);
        doReturn(Flux.empty()).when(cacheManager).getFromOffset(any(), any(), any());

        // connect 이벤트는 publisher를 거치지 않고 Flux에 직접 방출되므로 factory만 stub
        AlertMessage connectMsg = mock(AlertMessage.class);
        when(connectMsg.id()).thenReturn("connect-0");
        when(connectMsg.type()).thenReturn(DefaultAlertMessageType.CONNECT);
        when(connectMsg.body()).thenReturn("connected");
        when(alertMessageFactory.onConnect(eq(NAMESPACE), eq(SUBSCRIBER_ID), any()))
                .thenReturn(connectMsg);
    }

    private void triggerSubscribe(String lastEventId) {
        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), lastEventId, 30000L)
                .subscribe()
                .dispose();
    }

    @Test
    @DisplayName("lastEventId 없으면 getFromOffset 호출 안함")
    void noLastEventId_doesNotCallGetFromOffset() {
        triggerSubscribe(null);

        verify(cacheManager, never()).getFromOffset(any(), any(), any());
    }

    @Test
    @DisplayName("lastEventId=100이면 offset 100으로 getFromOffset 호출")
    void withLastEventId100_getFromOffsetWith100() {
        triggerSubscribe("100");

        verify(cacheManager).getFromOffset(CACHE_KEY, 100L, DefaultAlertMessage.class);
        verify(cacheManager).getFromOffset(BROADCAST_KEY, 100L, DefaultAlertMessage.class);
    }

    @Test
    @DisplayName("재전송 메시지는 Kafka를 거치지 않고 Flux로 직접 방출된다")
    void missedMessages_emittedDirectlyInFlux_notViaKafka() {
        AlertMessage missed = new DefaultAlertMessage(
                "103", NAMESPACE, List.of(AlertTarget.id(SUBSCRIBER_ID)),
                DefaultAlertMessageType.MESSAGE, "missed body", false, null);
        doReturn(Flux.just(missed)).when(cacheManager)
                .getFromOffset(CACHE_KEY, 100L, DefaultAlertMessage.class);

        StepVerifier.create(
                        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), "100", 30000L))
                .assertNext(event -> assertThat(event.event()).isEqualTo(DefaultAlertMessageType.CONNECT.toString()))  // connect 이벤트가 항상 첫 번째
                .assertNext(event -> {
                    assertThat(event.id()).isEqualTo("103");
                    assertThat(event.data()).isEqualTo("missed body");
                })
                .thenCancel()
                .verify();

        // recovery와 connect 모두 publisher를 거치지 않고 Flux에서 직접 방출된다
        verifyNoInteractions(alertMessagePublisher);
    }

    @Test
    @DisplayName("태그 포함 시 ID + 각 태그 캐시 키로 getFromOffset 호출")
    void withTags_getFromOffsetCalledForIdAndEachTag() {
        String tagKey1 = "alert:test-ns:tag:vip";
        String tagKey2 = "alert:test-ns:tag:admin";
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.id(SUBSCRIBER_ID))).thenReturn(CACHE_KEY);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.tag("vip"))).thenReturn(tagKey1);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.tag("admin"))).thenReturn(tagKey2);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.broadcast())).thenReturn(BROADCAST_KEY);
        doReturn(Flux.empty()).when(cacheManager).getFromOffset(any(), any(), any());

        manager.subscribe(channel, SUBSCRIBER_ID, List.of("vip", "admin"), "100", 30000L)
                .subscribe()
                .dispose();

        verify(cacheManager).getFromOffset(CACHE_KEY, 100L, DefaultAlertMessage.class);
        verify(cacheManager).getFromOffset(tagKey1, 100L, DefaultAlertMessage.class);
        verify(cacheManager).getFromOffset(tagKey2, 100L, DefaultAlertMessage.class);
        verify(cacheManager).getFromOffset(BROADCAST_KEY, 100L, DefaultAlertMessage.class);
    }
}

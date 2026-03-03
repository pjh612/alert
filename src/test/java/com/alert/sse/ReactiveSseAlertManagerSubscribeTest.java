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

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveSseAlertManagerSubscribeTest {

    @Mock
    ReactiveAlertMessagePublisher alertMessagePublisher;
    @Mock
    AlertMessageFactory alertMessageFactory;
    @Mock
    TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> emitterRepository;
    @Mock
    ReactiveAlertCacheManager alertCacheManager;
    @Mock
    AlertMessageSupport support;

    ReactiveSseAlertManager manager;

    private static final String NAMESPACE = "test-ns";
    private static final String SUBSCRIBER_ID = "user1";
    private static final String CACHE_KEY = "alert:test-ns:user:user1";

    private AlertChannel channel;
    private AlertMessage defaultConnectMsg;

    @BeforeEach
    void setUp() {
        manager = new ReactiveSseAlertManager(
                alertMessagePublisher, alertMessageFactory,
                emitterRepository, alertCacheManager, support, DefaultAlertMessage.class);

        channel = () -> NAMESPACE;

        when(support.resolveCacheKey(eq(NAMESPACE), any(AlertTarget.class))).thenReturn(CACHE_KEY);
        when(alertCacheManager.getFromOffset(any(), any(), any())).thenReturn(Flux.empty());

        // connect 이벤트는 publisher를 거치지 않고 Flux에 직접 방출되므로 factory만 stub
        defaultConnectMsg = mock(AlertMessage.class);
        when(defaultConnectMsg.id()).thenReturn("connect-0");
        when(defaultConnectMsg.type()).thenReturn(DefaultAlertMessageType.CONNECT);
        when(defaultConnectMsg.body()).thenReturn("connected");
        when(alertMessageFactory.onConnect(eq(NAMESPACE), eq(SUBSCRIBER_ID), any()))
                .thenReturn(defaultConnectMsg);
    }

    @Test
    @DisplayName("subscribe: connect 이벤트가 Flux 첫 번째 원소로 방출된다")
    void subscribe_emitsConnectEventAsFirstElement() {
        StepVerifier.create(manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L))
                .assertNext(event -> {
                    assertThat(event.id()).isEqualTo("connect-0");
                    assertThat(event.event()).isEqualTo(DefaultAlertMessageType.CONNECT.toString());
                })
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("subscribe: connect 이벤트는 alertMessagePublisher를 거치지 않는다")
    void subscribe_connectEvent_doesNotGoThroughPublisher() {
        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L)
                .subscribe()
                .dispose();

        verifyNoInteractions(alertMessagePublisher);
    }

    @Test
    @DisplayName("timeout 발생 시 에러가 아닌 정상 종료(onComplete)로 처리된다")
    void timeout_completesNormally_notError() {
        // 1ms timeout → 즉시 타임아웃 발생
        StepVerifier.withVirtualTime(() -> manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 1L))
                .expectSubscription()
                .assertNext(e -> assertThat(e.event()).isEqualTo(DefaultAlertMessageType.CONNECT.toString()))
                .thenAwait(Duration.ofMillis(2))
                .expectComplete()   // TimeoutException이 아닌 onComplete
                .verify();
    }
}

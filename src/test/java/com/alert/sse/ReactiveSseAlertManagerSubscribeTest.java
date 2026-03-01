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
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;

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

    @BeforeEach
    void setUp() {
        manager = new ReactiveSseAlertManager(
                alertMessagePublisher, alertMessageFactory,
                emitterRepository, alertCacheManager, support, DefaultAlertMessage.class);

        channel = () -> NAMESPACE;

        when(support.resolveCacheKey(eq(NAMESPACE), any(AlertTarget.class))).thenReturn(CACHE_KEY);
        when(alertCacheManager.getFromOffset(any(), any(), any())).thenReturn(Flux.empty());
    }

    @Test
    @DisplayName("subscribe: Flux 반환")
    void subscribe_returnsFlux() {
        StepVerifier.create(manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L))
                .expectNextCount(0)
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("subscribe: 연결 메시지 전송")
    void subscribe_sendsConnectMessage() {
        AlertMessage connectMsg = mock(AlertMessage.class);
        when(alertMessageFactory.onConnect(eq(NAMESPACE), eq(SUBSCRIBER_ID), any())).thenReturn(connectMsg);
        when(alertMessagePublisher.publish(eq(NAMESPACE), eq(connectMsg))).thenReturn(Mono.empty());

        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L)
                .subscribe();

        verify(alertMessagePublisher).publish(NAMESPACE, connectMsg);
    }
}

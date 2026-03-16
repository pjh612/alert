package com.alert.sse;

import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.comparator.DefaultIdComparator;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.id.SnowflakeIdGenerator;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessageFactory;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import com.alert.core.messaging.publisher.ReactiveAlertMessagePublisher;
import com.alert.core.session.AlertSession;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReactiveSseAlertManagerTest {

    @Mock
    private ReactiveAlertMessagePublisher alertMessagePublisher;
    @Mock
    private TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> repository;
    @Mock
    private ReactiveAlertCacheManager alertCacheManager;
    @Mock
    private AlertMessageSupport support;

    private AlertMessageFactory alertMessageFactory;
    private ReactiveSseAlertManager manager;

    private static final String NAMESPACE = "test-ns";
    private static final String SUBSCRIBER_ID = "user1";

    @BeforeEach
    void setUp() {
        alertMessageFactory = new DefaultAlertMessageFactory(new AlertMessageSupport(new SnowflakeIdGenerator(1L)));

        manager = new ReactiveSseAlertManager(
                alertMessagePublisher,
                alertMessageFactory,
                repository,
                alertCacheManager,
                support,
                AlertMessage.class,
                new DefaultIdComparator()
        );

        lenient().when(support.resolveCacheKey(eq(NAMESPACE), any(AlertTarget.class))).thenReturn("cache-key");
    }

    @Test
    @DisplayName("subscribe: connect 이벤트가 Flux 첫 번째 원소로 방출된다")
    void subscribe_emitsConnectEventAsFirstElement() {
        StepVerifier.create(manager.subscribe(() -> NAMESPACE, SUBSCRIBER_ID, List.of(), null, 30000L))
                .assertNext(event -> Assertions.assertThat(event.event()).isEqualTo(DefaultAlertMessageType.CONNECT.toString()))
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("이미 동일한 ID로 연결된 세션이 존재하면 기존 세션을 종료한다")
    void subscribe_completesExistingSession_whenSubscriberAlreadyExists() {
        // Given
        Sinks.Many<ServerSentEvent<Object>> oldSink = mock(Sinks.Many.class);
        AlertSession<Sinks.Many<ServerSentEvent<Object>>> oldSession = mock(AlertSession.class);

        when(oldSession.engine()).thenReturn(oldSink);
        when(repository.deleteById(NAMESPACE, SUBSCRIBER_ID)).thenReturn(oldSession);

        // When
        manager.subscribe(() -> NAMESPACE, SUBSCRIBER_ID, List.of(), null, 5000L);

        // Then
        verify(oldSink).tryEmitComplete();
    }

    @Test
    @DisplayName("과거 데이터 복구 범위에 포함된 중복 이벤트는 실시간 스트림에서 제외된다")
    void subscribe_filtersOutDuplicateLiveEvents() {
        // Given
        AlertMessage msg100 = createMsg("100");
        AlertMessage msg101 = createMsg("101");
        AlertMessage msg102 = createMsg("102");
        AlertMessage msg103 = createMsg("103");

        doReturn(Flux.just(msg101, msg102)).when(alertCacheManager).getFromOffset(any(), eq(msg100.id()), any());

        // When
        Flux<ServerSentEvent<Object>> resultFlux = manager.subscribe(() -> NAMESPACE, SUBSCRIBER_ID, List.of(), msg100.id(), 5000L);

        // Then
        StepVerifier.create(resultFlux)
                .expectNextMatches(e -> e.event().equals("CONNECT"))
                .expectNextMatches(e -> e.id().equals(msg101.id()))
                .expectNextMatches(e -> e.id().equals(msg102.id()))
                .then(() -> {
                    Sinks.Many<ServerSentEvent<Object>> sink = getSinkFromRepository(NAMESPACE, SUBSCRIBER_ID);
                    sink.tryEmitNext(ServerSentEvent.builder().id(msg102.id()).data("dupe").build());
                    sink.tryEmitNext(ServerSentEvent.builder().id(msg103.id()).data("new").build());
                })
                .expectNextMatches(e -> e.id().equals(msg103.id()))
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("lastEventId가 없으면 recovery 조회를 하지 않는다")
    void subscribe_doesNotQueryCache_whenLastEventIdNull() {
        // When
        manager.subscribe(() -> NAMESPACE, SUBSCRIBER_ID, List.of(), null, 5000L);

        // Then
        verify(alertCacheManager, never()).getFromOffset(any(), any(), any());
    }

    @Test
    @DisplayName("getRecoveryFlux: ID가 null인 메시지는 복구 목록에서 제외되어야 한다")
    void getRecoveryFlux_shouldFilterOutNullIdMessages() {
        // Given
        AlertMessage invalidMsg = mock(AlertMessage.class);
        when(invalidMsg.id()).thenReturn(null); // ID가 null인 메시지

        AlertMessage msg = createMsg("109");
        AlertMessage validMsg = createMsg("200");

        // 캐시에서 ID가 없는 놈과 있는 놈을 섞어서 반환
        doReturn(Flux.just(invalidMsg, validMsg))
                .when(alertCacheManager).getFromOffset(any(), eq(msg.id()), any());

        // When
        Flux<ServerSentEvent<Object>> result = manager.subscribe(() -> NAMESPACE, SUBSCRIBER_ID, List.of(), msg.id(), 5000L);

        // Then
        StepVerifier.create(result)
                .expectNextMatches(e -> e.event().equals("CONNECT"))
                .expectNextMatches(e -> e.id().equals(validMsg.id()))
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("isAfterRecovery: lastEventId가 있을 때 실시간 이벤트 ID가 null이면 필터링되어야 한다")
    void isAfterRecovery_shouldFilterOut_whenEventIdIsNullWithOffset() {
        // Given
        AlertMessage msg = createMsg("90");
        AlertMessage recoveryMsg = createMsg("100");
        AlertMessage newMsg = createMsg("101");
        doReturn(Flux.just(recoveryMsg))
                .when(alertCacheManager).getFromOffset(any(), eq(msg.id()), any());

        // When
        Flux<ServerSentEvent<Object>> result = manager.subscribe(() -> NAMESPACE, SUBSCRIBER_ID, List.of(), msg.id(), 5000L);

        // Then
        StepVerifier.create(result)
                .expectNextMatches(e -> e.event().equals("CONNECT"))
                .expectNextMatches(e -> e.id().equals(recoveryMsg.id())) // 복구 데이터
                .then(() -> {
                    Sinks.Many<ServerSentEvent<Object>> sink = getSinkFromRepository(NAMESPACE, SUBSCRIBER_ID);
                    sink.tryEmitNext(ServerSentEvent.builder().id(null).data("dropped").build());
                    sink.tryEmitNext(ServerSentEvent.builder().id(newMsg.id()).data("kept").build());
                })
                .expectNextMatches(e -> newMsg.id().equals(e.id()))
                .thenCancel()
                .verify();
    }


    @Test
    @DisplayName("isAfterRecovery: 복구 데이터가 없어 maxId가 null인 경우 false를 반환해야 한다")
    void isAfterRecovery_shouldReturnFalse_whenMaxIdIsNull() {
        // Given
        AlertMessage msg = createMsg("90");
        AlertMessage msg2 = createMsg("100");
        when(alertCacheManager.getFromOffset(any(), any(), any())).thenReturn(Flux.empty());

        // When
        Flux<ServerSentEvent<Object>> result = manager.subscribe(() -> NAMESPACE, SUBSCRIBER_ID, List.of(), msg.id(), 5000L);

        // Then
        StepVerifier.create(result)
                .expectNextMatches(e -> e.event().equals("CONNECT"))
                .then(() -> {
                    Sinks.Many<ServerSentEvent<Object>> sink = getSinkFromRepository(NAMESPACE, SUBSCRIBER_ID);
                    sink.tryEmitNext(ServerSentEvent.builder().id(msg2.id()).data("live").build());
                })
                .expectNoEvent(Duration.ofMillis(500))
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("sortRecovery=false일 때 복구 메시지가 정렬되지 않는다 (캐시 반환 순서 유지)")
    void recovery_withoutSort_preservesOriginalOrder() {
        AlertMessage msg102 = new com.alert.core.messaging.model.DefaultAlertMessage(
                "102", NAMESPACE, List.of(AlertTarget.id(SUBSCRIBER_ID)),
                DefaultAlertMessageType.MESSAGE, "b1", false, null);
        AlertMessage msg100 = new com.alert.core.messaging.model.DefaultAlertMessage(
                "100", NAMESPACE, List.of(AlertTarget.id(SUBSCRIBER_ID)),
                DefaultAlertMessageType.MESSAGE, "b2", false, null);

        // 102가 100보다 먼저 반환
        doReturn(Flux.just(msg102, msg100))
                .when(alertCacheManager).getFromOffset(any(), eq("99"), any());

        Flux<ServerSentEvent<Object>> result = manager.subscribe(() -> NAMESPACE, SUBSCRIBER_ID, List.of(), "99", 5000L);

        StepVerifier.create(result)
                .expectNextMatches(e -> e.event().equals("CONNECT"))
                .expectNextMatches(e -> e.id().equals("102"))
                .expectNextMatches(e -> e.id().equals("100"))
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("sortRecovery=true일 때 복구 메시지가 idComparator로 정렬된다")
    void recovery_withSort_sortsMessages() {
        ReactiveSseAlertManager sortedManager = new ReactiveSseAlertManager(
                alertMessagePublisher, alertMessageFactory, repository,
                alertCacheManager, support, AlertMessage.class,
                new DefaultIdComparator(), true
        );
        lenient().when(support.resolveCacheKey(eq(NAMESPACE), any(AlertTarget.class))).thenReturn("cache-key");

        AlertMessage msg102 = new com.alert.core.messaging.model.DefaultAlertMessage(
                "102", NAMESPACE, List.of(AlertTarget.id(SUBSCRIBER_ID)),
                DefaultAlertMessageType.MESSAGE, "b1", false, null);
        AlertMessage msg100 = new com.alert.core.messaging.model.DefaultAlertMessage(
                "100", NAMESPACE, List.of(AlertTarget.id(SUBSCRIBER_ID)),
                DefaultAlertMessageType.MESSAGE, "b2", false, null);

        doReturn(Flux.just(msg102, msg100))
                .when(alertCacheManager).getFromOffset(any(), eq("99"), any());

        Flux<ServerSentEvent<Object>> result = sortedManager.subscribe(() -> NAMESPACE, SUBSCRIBER_ID, List.of(), "99", 5000L);

        StepVerifier.create(result)
                .expectNextMatches(e -> e.event().equals("CONNECT"))
                .expectNextMatches(e -> e.id().equals("100"))
                .expectNextMatches(e -> e.id().equals("102"))
                .thenCancel()
                .verify();
    }

    // --- Helper Methods ---

    private AlertMessage createMsg(String data) {
        return alertMessageFactory.create(NAMESPACE,
                List.of(AlertTarget.id(SUBSCRIBER_ID)),
                DefaultAlertMessageType.MESSAGE, "data-" + data, null);
    }

    private Sinks.Many<ServerSentEvent<Object>> getSinkFromRepository(String ns, String id) {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Sinks.Many<ServerSentEvent<Object>>> captor = ArgumentCaptor.forClass(Sinks.Many.class);
        verify(repository, atLeastOnce()).put(eq(ns), eq(id), any(), captor.capture());
        return captor.getValue();
    }
}
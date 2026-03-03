package com.alert.infra.redis;

import com.alert.core.messaging.broadcaster.MessageConverter;
import com.alert.core.messaging.broadcaster.ReactiveAlertMessageHandler;
import com.alert.core.messaging.model.AlertMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveRedisMessageListenerRegistrarTest {

    @Mock
    ReactiveRedisMessageListenerContainer container;
    @Mock
    ReactiveAlertMessageHandler handler;
    @Mock
    MessageConverter<String, AlertMessage> messageConverter;

    static final String TOPIC = "test-topic";

    ReactiveRedisMessageListenerRegistrar registrar;

    @BeforeEach
    void setUp() {
        registrar = new ReactiveRedisMessageListenerRegistrar(container);
        when(container.receive(any(ChannelTopic.class))).thenReturn(Flux.never());
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("start 전 isRunning = false")
    void beforeStart_isRunning_false() {
        assertThat(registrar.isRunning()).isFalse();
    }

    @Test
    @DisplayName("getPhase = Integer.MAX_VALUE - 1")
    void getPhase_returnsMaxValueMinusOne() {
        assertThat(registrar.getPhase()).isEqualTo(Integer.MAX_VALUE - 1);
    }

    @Test
    @DisplayName("start 후 isRunning = true")
    void afterStart_isRunning_true() {
        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();

        assertThat(registrar.isRunning()).isTrue();
    }

    @Test
    @DisplayName("stop 후 isRunning = false")
    void afterStop_isRunning_false() {
        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();
        registrar.stop();

        assertThat(registrar.isRunning()).isFalse();
    }

    @Test
    @DisplayName("start 중복 호출 시 container.receive는 한 번만 호출")
    void start_idempotent_singleSubscription() {
        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();
        registrar.start();  // 두 번째 호출 무시

        verify(container, times(1)).receive(any(ChannelTopic.class));
    }

    @Test
    @DisplayName("running 중 동일 topic 재등록 → 기존 리스너 중단 후 신규 구독")
    void register_sameTopic_whileRunning_replacesListener() {
        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();
        registrar.register(TOPIC, handler, messageConverter);  // 재등록

        // start 시 1회 + 재등록 시 1회 = 총 2회
        verify(container, times(2)).receive(any(ChannelTopic.class));
    }

    @Test
    @DisplayName("stop 후 재시작 시 리스너 정상 복구")
    void stopThenStart_listenersRestarted() {
        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();
        registrar.stop();
        registrar.start();

        assertThat(registrar.isRunning()).isTrue();
        verify(container, times(2)).receive(any(ChannelTopic.class));
    }

    // ── Graceful drain ─────────────────────────────────────────────────────

    @Test
    @DisplayName("stop 호출 시 처리 중인 메시지가 완료된 후 종료")
    @SuppressWarnings("unchecked")
    void stop_drainsInFlightMessagesBeforeStopping() throws InterruptedException {
        CountDownLatch handlerStarted = new CountDownLatch(1);
        AtomicBoolean handlerCompleted = new AtomicBoolean(false);

        Sinks.Many<ReactiveSubscription.Message<String, String>> redisSink =
                Sinks.many().unicast().onBackpressureBuffer();
        when(container.receive(any(ChannelTopic.class))).thenReturn(redisSink.asFlux());

        AlertMessage alertMsg = mock(AlertMessage.class);
        ReactiveSubscription.Message<String, String> channelMsg = mock(ReactiveSubscription.Message.class);
        when(channelMsg.getMessage()).thenReturn("payload");
        when(messageConverter.convert("payload")).thenReturn(alertMsg);

        // handler는 300ms 소요 — stop() 호출 시점에 여전히 처리 중
        when(handler.handle(alertMsg)).thenReturn(
                Mono.delay(Duration.ofMillis(300))
                        .doOnSubscribe(s -> handlerStarted.countDown())
                        .doOnTerminate(() -> handlerCompleted.set(true))
                        .then()
        );

        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();

        // Act: 메시지 방출 후 handler가 시작될 때까지 대기
        redisSink.tryEmitNext(channelMsg);
        assertThat(handlerStarted.await(1, TimeUnit.SECONDS))
                .as("handler가 1초 내에 시작되어야 함")
                .isTrue();

        // stop()은 handler가 완료될 때까지 블로킹해야 한다
        registrar.stop();

        // Assert: stop() 반환 후 handler가 완료된 상태여야 함
        assertThat(handlerCompleted.get())
                .as("stop() 반환 시 처리 중인 메시지가 이미 완료되어 있어야 함")
                .isTrue();
        assertThat(registrar.isRunning()).isFalse();
    }
}

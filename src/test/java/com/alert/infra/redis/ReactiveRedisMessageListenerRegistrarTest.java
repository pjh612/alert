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

    @Test
    @DisplayName("메시지 변환 에러: 페이로드 변환 중 예외가 발생해도 스트림이 유지되어야 함")
    @SuppressWarnings("unchecked")
    void conversionError_payloadInvalid_logsErrorAndContinuesStream() throws InterruptedException {
        // Given: 첫 번째 메시지는 에러 유발, 두 번째는 정상
        Sinks.Many<ReactiveSubscription.Message<String, String>> redisSink =
                Sinks.many().unicast().onBackpressureBuffer();
        when(container.receive(any(ChannelTopic.class))).thenReturn(redisSink.asFlux());

        ReactiveSubscription.Message<String, String> errorMsg = mock(ReactiveSubscription.Message.class);
        ReactiveSubscription.Message<String, String> normalMsg = mock(ReactiveSubscription.Message.class);

        when(errorMsg.getMessage()).thenReturn("invalid-json");
        when(normalMsg.getMessage()).thenReturn("valid-json");

        // 첫 번째 호출 시 RuntimeException 던짐 (try-catch 블록 검증)
        when(messageConverter.convert("invalid-json")).thenThrow(new RuntimeException("변환 실패"));

        AlertMessage alertMsg = mock(AlertMessage.class);
        when(messageConverter.convert("valid-json")).thenReturn(alertMsg);

        CountDownLatch latch = new CountDownLatch(1);
        when(handler.handle(alertMsg)).thenReturn(Mono.fromRunnable(latch::countDown));

        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();

        // Act
        redisSink.tryEmitNext(errorMsg);  // 예외 발생
        redisSink.tryEmitNext(normalMsg); // 후속 메시지 전송

        // Assert
        assertThat(latch.await(2, TimeUnit.SECONDS))
                .as("변환 에러가 발생한 후에도 스트림이 유지되어 다음 메시지를 처리해야 함")
                .isTrue();
    }

    @Test
    @DisplayName("핸들러 처리 에러: 비즈니스 로직 에러(Mono.error) 발생 시에도 스트림이 유지되어야 함")
    @SuppressWarnings("unchecked")
    void handlerError_handleReturnsError_logsErrorAndContinuesStream() throws InterruptedException {
        // Given
        Sinks.Many<ReactiveSubscription.Message<String, String>> redisSink =
                Sinks.many().unicast().onBackpressureBuffer();
        when(container.receive(any(ChannelTopic.class))).thenReturn(redisSink.asFlux());

        ReactiveSubscription.Message<String, String> msg1 = mock(ReactiveSubscription.Message.class);
        ReactiveSubscription.Message<String, String> msg2 = mock(ReactiveSubscription.Message.class);
        when(msg1.getMessage()).thenReturn("p1");
        when(msg2.getMessage()).thenReturn("p2");

        AlertMessage alertMsg1 = mock(AlertMessage.class);
        AlertMessage alertMsg2 = mock(AlertMessage.class);
        when(messageConverter.convert("p1")).thenReturn(alertMsg1);
        when(messageConverter.convert("p2")).thenReturn(alertMsg2);

        // 첫 번째 핸들러는 Mono 에러 반환 (onErrorResume 블록 검증)
        when(handler.handle(alertMsg1)).thenReturn(Mono.error(new RuntimeException("핸들러 실패")));

        CountDownLatch latch = new CountDownLatch(1);
        when(handler.handle(alertMsg2)).thenReturn(Mono.fromRunnable(latch::countDown));

        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();

        // Act
        redisSink.tryEmitNext(msg1);
        redisSink.tryEmitNext(msg2);

        // Assert
        assertThat(latch.await(2, TimeUnit.SECONDS))
                .as("핸들러 에러가 발생해도 스트림이 종료되지 않고 다음 데이터를 처리해야 함")
                .isTrue();
    }

    @Test
    @DisplayName("치명적 스트림 에러: 리시버 자체에서 에러 발생 시 우아하게 종료되어야 함")
    void criticalStreamError_receiveFails_logsErrorAndCompletesGracefully() {
        // Given: Redis 연결 끊김 등 스트림 소스 자체가 에러인 상황
        when(container.receive(any(ChannelTopic.class)))
                .thenReturn(Flux.error(new RuntimeException("Redis 연결 유실")));

        // Act
        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();

        // Assert: doOnError와 onErrorComplete 로직을 통과하고 종료되어야 함
        registrar.stop();
        assertThat(registrar.isRunning()).isFalse();
    }

    @Test
    @DisplayName("중단 신호 전송: 등록되지 않은 토픽에 대해 signalStop 호출 시 아무 일도 일어나지 않아야 함")
    void signalStop_nonExistentTopic_doesNothing() {
        // Given: 아무것도 등록되지 않은 상태

        // When
        registrar.stop();

        // Then
        assertThat(registrar.isRunning()).isFalse();
    }

    @Test
    @DisplayName("중단 신호 전송: 이미 종료된 토픽에 대해 다시 signalStop 호출 시 안전하게 무시되어야 함")
    void signalStop_alreadyStoppedTopic_handledGracefully() {
        // Given
        registrar.register(TOPIC, handler, messageConverter);
        registrar.start();

        // When
        registrar.stop(); // 첫 번째 호출에서 sink가 제거됨
        registrar.stop(); // 두 번째 호출에서 sink는 null이 됨 (이때 false 분기 실행)

        // Then
        assertThat(registrar.isRunning()).isFalse();
    }
}

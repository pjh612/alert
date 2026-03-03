package com.alert.infra.redis;

import com.alert.core.messaging.model.AlertMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers
class ReactiveRedisMessageListenerRegistrarIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    // 각 테스트마다 새로운 실제 Redis 연결을 사용한다
    private LettuceConnectionFactory connectionFactory;
    private ReactiveRedisMessageListenerContainer container;
    private ReactiveRedisMessageListenerRegistrar registrar;
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        connectionFactory = new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        container = new ReactiveRedisMessageListenerContainer(connectionFactory);
        registrar = new ReactiveRedisMessageListenerRegistrar(container);
        registrar.start();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
    }

    @AfterEach
    void tearDown() {
        if (registrar.isRunning()) {
            registrar.stop();
        }
        container.destroy();
        connectionFactory.destroy();
    }

    // ── 실제 메시지 수신 ──────────────────────────────────────────────────

    @Test
    @DisplayName("실제 Redis Pub/Sub 메시지가 핸들러에 전달된다")
    void realRedis_publishedMessage_isDeliveredToHandler() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(1);
        AtomicBoolean handlerCalled = new AtomicBoolean(false);

        registrar.register("topic:delivery",
                alertMsg -> {
                    handlerCalled.set(true);
                    received.countDown();
                    return Mono.empty();
                },
                payload -> mock(AlertMessage.class));

        // 구독이 Redis에 반영될 시간을 준다
        Thread.sleep(100);
        redisTemplate.convertAndSend("topic:delivery", "hello");

        assertThat(received.await(3, TimeUnit.SECONDS))
                .as("3초 내에 메시지를 수신해야 함")
                .isTrue();
        assertThat(handlerCalled.get()).isTrue();
    }

    // ── Spring SmartLifecycle 자동 호출 ───────────────────────────────────

    @Test
    @DisplayName("Spring 컨텍스트 종료 시 stop()이 자동으로 호출된다")
    void springContextClose_automaticallyCalls_stop() {
        GenericApplicationContext ctx = new GenericApplicationContext();
        ctx.getBeanFactory().registerSingleton("registrar", registrar);
        ctx.refresh();  // Spring이 SmartLifecycle.start() 호출 (이미 running이므로 idempotent)

        assertThat(registrar.isRunning()).isTrue();
        ctx.close();    // Spring이 SmartLifecycle.stop() 호출
        assertThat(registrar.isRunning()).isFalse();
    }

    // ── 종료 순서 ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("Kafka(MAX_VALUE)가 Redis(MAX_VALUE-1)보다 먼저 종료된다")
    void springShutdown_kafkaStopsBeforeRedis() {
        List<String> stopOrder = Collections.synchronizedList(new ArrayList<>());

        // Kafka registrar 역할을 하는 SmartLifecycle (phase = MAX_VALUE)
        SmartLifecycle kafkaLifecycle = new SmartLifecycle() {
            private boolean kafkaRunning = false;

            @Override public void start() { kafkaRunning = true; }
            @Override public void stop()  { kafkaRunning = false; stopOrder.add("kafka"); }
            @Override public boolean isRunning() { return kafkaRunning; }
            @Override public int getPhase() { return Integer.MAX_VALUE; }
        };

        // Redis registrar: stop() 시 순서 기록
        ReactiveRedisMessageListenerContainer mockContainer = mock(ReactiveRedisMessageListenerContainer.class);
        when(mockContainer.receive(any(ChannelTopic.class))).thenReturn(Flux.never());
        ReactiveRedisMessageListenerRegistrar redisRegistrar =
                new ReactiveRedisMessageListenerRegistrar(mockContainer) {
                    @Override
                    public synchronized void stop() {
                        super.stop();
                        stopOrder.add("redis");
                    }
                };

        GenericApplicationContext ctx = new GenericApplicationContext();
        ctx.getBeanFactory().registerSingleton("kafkaLifecycle", kafkaLifecycle);
        ctx.getBeanFactory().registerSingleton("redisRegistrar", redisRegistrar);
        ctx.refresh();
        ctx.close();  // phase 내림차순(MAX_VALUE → MAX_VALUE-1)으로 stop() 호출

        assertThat(stopOrder)
                .as("Kafka가 먼저 종료되고 Redis가 나중에 종료되어야 함")
                .containsExactly("kafka", "redis");
    }

    // ── 실제 Redis + graceful drain ───────────────────────────────────────

    @Test
    @DisplayName("컨텍스트 종료 시 실제로 처리 중인 메시지가 완료된 후 종료된다")
    void springContextClose_drainsRealInFlightMessage() throws Exception {
        CountDownLatch handlerStarted = new CountDownLatch(1);
        AtomicBoolean handlerCompleted = new AtomicBoolean(false);

        // proceedSink: 테스트가 신호를 보낼 때까지 핸들러를 의도적으로 멈춰둔다
        Sinks.One<Void> proceedSink = Sinks.one();

        registrar.register("topic:drain",
                alertMsg -> Mono.fromRunnable(handlerStarted::countDown)
                        .then(proceedSink.asMono())        // 신호가 올 때까지 핸들러 블로킹
                        .doOnTerminate(() -> handlerCompleted.set(true))
                        .then(),
                payload -> mock(AlertMessage.class));

        Thread.sleep(100);
        redisTemplate.convertAndSend("topic:drain", "drain-payload");
        assertThat(handlerStarted.await(3, TimeUnit.SECONDS))
                .as("3초 내에 핸들러가 시작되어야 함")
                .isTrue();

        // close()는 stop()이 완료될 때까지 블로킹하므로 별도 스레드에서 실행
        GenericApplicationContext ctx = new GenericApplicationContext();
        ctx.getBeanFactory().registerSingleton("registrar", registrar);
        ctx.refresh();
        CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(ctx::close);

        // stop() 진입을 기다린다 (running = false는 stop() 첫 번째 라인에서 설정됨)
        await().atMost(Duration.ofSeconds(3)).until(() -> !registrar.isRunning());

        // 이 시점: stop()은 Mono.when(...).block() 에서 handler 완료를 대기 중
        // → close()는 아직 반환되지 않았음을 검증
        assertThat(closeFuture.isDone())
                .as("handler 미완료 상태에서 close()는 반환되면 안 됨")
                .isFalse();
        assertThat(handlerCompleted.get())
                .as("proceedSink 신호 전에는 handler가 완료되지 않아야 함")
                .isFalse();

        // 핸들러 완료 허용 → stop()의 block() 반환 → close() 반환
        proceedSink.tryEmitEmpty();
        closeFuture.get(5, TimeUnit.SECONDS);

        assertThat(handlerCompleted.get())
                .as("close() 반환 후 handler가 완료되어 있어야 함")
                .isTrue();
    }
}

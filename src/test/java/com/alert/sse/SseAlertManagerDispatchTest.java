package com.alert.sse;

import com.alert.core.cache.AlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.publisher.AlertMessagePublisher;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SseAlertManagerDispatchTest {

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

    @Mock
    Executor executor;

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
                DefaultAlertMessage.class,
                executor
        );

        channel = () -> NAMESPACE;
    }

    @Test
    @DisplayName("subscribe 호출 시 emitter가 저장되고 반환된다")
    void shouldStoreAndReturnEmitter_whenSubscribe() {
        when(alertMessageFactory.onConnect(any(), any(), any()))
                .thenReturn(org.mockito.Mockito.mock(AlertMessage.class));

        SseEmitter emitter = manager.subscribe(channel, SUBSCRIBER_ID, List.of("vip"), null, 30000L);

        assertThat(emitter).isNotNull();
        verify(emitterRepository).put(
                eq(NAMESPACE),
                eq(SUBSCRIBER_ID),
                eq(new HashSet<>(List.of("vip"))),
                eq(emitter)
        );
    }

    @Test
    @DisplayName("subscribe 호출 시 connect 메시지 발행은 executor를 통해 위임된다")
    void shouldDelegateConnectPublishToExecutor_whenSubscribe() {
        AlertMessage connectMessage = org.mockito.Mockito.mock(AlertMessage.class);
        when(alertMessageFactory.onConnect(any(), any(), any())).thenReturn(connectMessage);

        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).execute(runnableCaptor.capture());

        runnableCaptor.getValue().run();

        verify(alertMessagePublisher).publish(NAMESPACE, connectMessage);
        verify(alertCacheManager, never()).getFromOffset(any(), any(), any());
    }

    @Test
    @DisplayName("lastEventId가 있으면 replay 작업도 executor를 통해 위임된다")
    void shouldDelegateReplayToExecutor_whenLastEventIdExists() {
        AlertMessage connectMessage = org.mockito.Mockito.mock(AlertMessage.class);
        when(alertMessageFactory.onConnect(any(), any(), any())).thenReturn(connectMessage);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.broadcast())).thenReturn(BROADCAST_KEY);
        when(support.resolveCacheKey(NAMESPACE, AlertTarget.id(SUBSCRIBER_ID))).thenReturn(CACHE_KEY);
        when(alertCacheManager.getFromOffset(any(), any(), any())).thenReturn(List.of());

        manager.subscribe(channel, SUBSCRIBER_ID, List.of(), "100", 30000L);

        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor, times(2)).execute(runnableCaptor.capture());

        List<Runnable> tasks = runnableCaptor.getAllValues();
        tasks.forEach(Runnable::run);

        verify(alertMessagePublisher).publish(NAMESPACE, connectMessage);
        verify(alertCacheManager).getFromOffset(CACHE_KEY, "100", DefaultAlertMessage.class);
        verify(alertCacheManager).getFromOffset(BROADCAST_KEY, "100", DefaultAlertMessage.class);
    }

    @Test
    @DisplayName("subscribe 호출 시 virtual thread executor를 사용하면 실제 virtual thread에서 connect 발행이 수행된다")
    void shouldPublishConnectMessageOnVirtualThread_whenUsingVirtualThreadExecutor() throws Exception {
        AlertMessage connectMessage = org.mockito.Mockito.mock(AlertMessage.class);
        when(alertMessageFactory.onConnect(any(), any(), any())).thenReturn(connectMessage);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executedOnVirtualThread = new AtomicBoolean(false);

        doAnswer(invocation -> {
            executedOnVirtualThread.set(Thread.currentThread().isVirtual());
            latch.countDown();
            return null;
        }).when(alertMessagePublisher).publish(NAMESPACE, connectMessage);

        try (ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            SseAlertManager virtualThreadManager = new SseAlertManager(
                    alertMessagePublisher,
                    alertMessageFactory,
                    emitterRepository,
                    alertCacheManager,
                    support,
                    DefaultAlertMessage.class,
                    virtualThreadExecutor
            );

            virtualThreadManager.subscribe(channel, SUBSCRIBER_ID, List.of(), null, 30000L);

            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(executedOnVirtualThread.get()).isTrue();
        }
    }
}

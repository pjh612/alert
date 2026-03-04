package com.alert.core.manager;

import static org.junit.jupiter.api.Assertions.*;

import com.alert.core.messaging.model.*;
import com.alert.core.messaging.publisher.ReactiveAlertMessagePublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReactiveAbstractAlertManagerTest {

    @Mock
    private AlertMessageFactory alertMessageFactory;
    @Mock
    private ReactiveAlertMessagePublisher alertMessagePublisher;
    @Mock
    private AlertChannel alertChannel;
    @Mock
    private AlertMessage alertMessage;

    private TestAlertManager manager;

    private static final String NAMESPACE = "test-ns";
    private static final String CHANNEL_NAME = "test-channel";

    // 추상 클래스 테스트를 위한 구체 클래스
    static class TestAlertManager extends ReactiveAbstractAlertManager {
        protected TestAlertManager(AlertMessageFactory factory, ReactiveAlertMessagePublisher publisher) {
            super(factory, publisher);
        }
    }

    @BeforeEach
    void setUp() {
        manager = new TestAlertManager(alertMessageFactory, alertMessagePublisher);

        // 공통 설정: 채널 정보
        when(alertChannel.namespace()).thenReturn(NAMESPACE);
        when(alertChannel.name()).thenReturn(CHANNEL_NAME);
    }

    @Test
    @DisplayName("notice(id): 특정 ID 타겟으로 메시지를 생성하고 발행해야 한다")
    void noticeById_shouldCreateAndPublish() {
        // Given
        String targetId = "user-123";
        Object payload = "Hello ID";

        when(alertMessageFactory.create(eq(NAMESPACE), anyList(), eq(DefaultAlertMessageType.MESSAGE), eq(payload), any()))
                .thenReturn(alertMessage);
        when(alertMessagePublisher.publish(eq(CHANNEL_NAME), eq(alertMessage))).thenReturn(Mono.empty());

        // When
        Mono<Void> result = manager.notice(alertChannel, targetId, payload);

        // Then
        StepVerifier.create(result)
                .verifyComplete();

        // 타겟이 ID로 생성되었는지 검증
        ArgumentCaptor<List<AlertTarget>> captor = ArgumentCaptor.forClass(List.class);
        verify(alertMessageFactory).create(any(), captor.capture(), any(), any(), any());

        List<AlertTarget> targets = captor.getValue();
        assertThat(targets).hasSize(1);
        assertThat(targets.get(0).type()).isEqualTo(AlertTarget.TargetType.ID);
        assertThat(targets.get(0).value()).isEqualTo(targetId);
    }

    @Test
    @DisplayName("noticeByTag: 특정 Tag 타겟으로 메시지를 생성하고 발행해야 한다")
    void noticeByTag_shouldCreateAndPublish() {
        // Given
        String tag = "admin-group";
        Object payload = "Hello Tag";

        when(alertMessageFactory.create(any(), any(), any(), any(), any())).thenReturn(alertMessage);
        when(alertMessagePublisher.publish(any(), any())).thenReturn(Mono.empty());

        // When
        Mono<Void> result = manager.noticeByTag(alertChannel, tag, payload);

        // Then
        StepVerifier.create(result).verifyComplete();

        ArgumentCaptor<List<AlertTarget>> captor = ArgumentCaptor.forClass(List.class);
        verify(alertMessageFactory).create(any(), captor.capture(), any(), any(), any());
        assertThat(captor.getValue().get(0).type()).isEqualTo(AlertTarget.TargetType.TAG);
    }

    @Test
    @DisplayName("broadcast: 모든 사용자 타겟으로 메시지를 생성하고 발행해야 한다")
    void broadcast_shouldCreateAndPublish() {
        // Given
        Object payload = "Hello Everyone";

        when(alertMessageFactory.create(any(), any(), any(), any(), any())).thenReturn(alertMessage);
        when(alertMessagePublisher.publish(any(), any())).thenReturn(Mono.empty());

        // When
        Mono<Void> result = manager.broadcast(alertChannel, payload);

        // Then
        StepVerifier.create(result).verifyComplete();

        ArgumentCaptor<List<AlertTarget>> captor = ArgumentCaptor.forClass(List.class);
        verify(alertMessageFactory).create(any(), captor.capture(), any(), any(), any());
        assertThat(captor.getValue().get(0).type()).isEqualTo(AlertTarget.TargetType.BROADCAST);
    }

    @Test
    @DisplayName("publish 실패 시 에러를 전파해야 한다")
    void notice_shouldPropagateError_whenPublishFails() {
        // Given
        Object payload = "Fail Message";
        RuntimeException error = new RuntimeException("Kafka Down");

        when(alertMessageFactory.create(any(), any(), any(), any(), any())).thenReturn(alertMessage);
        when(alertMessagePublisher.publish(any(), any())).thenReturn(Mono.error(error));

        // When
        Mono<Void> result = manager.notice(alertChannel, "target", payload);

        // Then
        StepVerifier.create(result)
                .expectErrorMatches(e -> e instanceof RuntimeException && e.getMessage().equals("Kafka Down"))
                .verify();

        // 로그 에러 처리 로직(doOnError)이 실행되었는지 확인
        verify(alertMessagePublisher).publish(any(), any());
    }
}
package com.alert.infra.kafka;

import com.alert.config.AlertKafkaProperties;
import com.alert.core.messaging.consumer.ReactiveTopicAlertMessageHandler;
import com.alert.core.messaging.model.AlertMessage;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveKafkaMessageConsumerRegistrarTest {

    @Mock ReactiveTopicAlertMessageHandler<Void> handler;
    @Mock KafkaSender<String, AlertMessage> kafkaSender;
    @Mock ReceiverRecord<String, AlertMessage> record;
    @Mock ReceiverOffset receiverOffset;

    // 테스트 속도를 위해 interval=1ms, maxAttempts=0 (재시도 없음)
    static final AlertKafkaProperties.DlqProperties DLQ_PROPS =
            new AlertKafkaProperties.DlqProperties(".dlq",
                    new AlertKafkaProperties.BackoffProperties(1L, 0L));

    static final String TOPIC = "test-topic";
    static final String DLQ_TOPIC = "test-topic.dlq";

    ReactiveKafkaMessageConsumerRegistrar<Void> registrar;

    @BeforeEach
    void setUp() {
        registrar = new ReactiveKafkaMessageConsumerRegistrar<>(Map.of(), DLQ_PROPS, kafkaSender);

        when(record.topic()).thenReturn(TOPIC);
        when(record.value()).thenReturn(mock(AlertMessage.class));
        when(record.key()).thenReturn("msg-key");
        when(record.headers()).thenReturn(new RecordHeaders());
        when(record.receiverOffset()).thenReturn(receiverOffset);
        when(receiverOffset.commit()).thenReturn(Mono.empty());
    }

    @Nested
    @DisplayName("Lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("start 전 isRunning = false")
        void beforeStart_isRunning_false() {
            assertThat(registrar.isRunning()).isFalse();
        }

        @Test
        @DisplayName("getPhase = Integer.MAX_VALUE")
        void getPhase_returnsMaxValue() {
            assertThat(registrar.getPhase()).isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        @DisplayName("start 후 isRunning = true")
        void afterStart_isRunning_true() {
            try (MockedStatic<KafkaReceiver> mocked = mockStatic(KafkaReceiver.class)) {
                stubKafkaReceiver(mocked);

                registrar.register(TOPIC, handler, 1);
                registrar.start();

                assertThat(registrar.isRunning()).isTrue();
            }
        }

        @Test
        @DisplayName("stop 후 isRunning = false")
        void afterStop_isRunning_false() {
            try (MockedStatic<KafkaReceiver> mocked = mockStatic(KafkaReceiver.class)) {
                stubKafkaReceiver(mocked);

                registrar.register(TOPIC, handler, 1);
                registrar.start();
                registrar.stop();

                assertThat(registrar.isRunning()).isFalse();
            }
        }

        @Test
        @DisplayName("start 중복 호출 시 KafkaReceiver는 한 번만 생성")
        void start_idempotent_singleConsumer() {
            try (MockedStatic<KafkaReceiver> mocked = mockStatic(KafkaReceiver.class)) {
                stubKafkaReceiver(mocked);

                registrar.register(TOPIC, handler, 1);
                registrar.start();
                registrar.start();

                mocked.verify(() -> KafkaReceiver.create(any()), times(1));
            }
        }

        @Test
        @DisplayName("running 중 동일 topic 재등록 → 기존 consumer dispose 후 신규 생성")
        void register_sameTopic_whileRunning_replacesConsumer() {
            try (MockedStatic<KafkaReceiver> mocked = mockStatic(KafkaReceiver.class)) {
                stubKafkaReceiver(mocked);

                registrar.register(TOPIC, handler, 1);
                registrar.start();
                registrar.register(TOPIC, handler, 1); // 동일 topic 재등록

                // start 시 1회 + 재등록 시 1회 = 총 2회
                mocked.verify(() -> KafkaReceiver.create(any()), times(2));
            }
        }

        @Test
        @DisplayName("stop 후 재시작 시 consumer 정상 복구")
        void stopThenStart_consumersRestarted() {
            try (MockedStatic<KafkaReceiver> mocked = mockStatic(KafkaReceiver.class)) {
                stubKafkaReceiver(mocked);

                registrar.register(TOPIC, handler, 1);
                registrar.start();
                registrar.stop();
                registrar.start();

                assertThat(registrar.isRunning()).isTrue();
                // start 1회 + restart 1회 = 총 2회
                mocked.verify(() -> KafkaReceiver.create(any()), times(2));
            }
        }
    }

    @Nested
    @DisplayName("processMessage")
    class ProcessMessage {

        @Test
        @DisplayName("handle 성공 → commit 1회, DLQ 미전송")
        void handle_success_commitsOnce_noDlq() {
            when(handler.handle(any(), any())).thenReturn(Mono.empty());

            StepVerifier.create(registrar.processMessage(record, handler, DLQ_TOPIC))
                    .verifyComplete();

            verify(receiverOffset, times(1)).commit();
            verify(kafkaSender, never()).send(any());
        }

        @Test
        @DisplayName("handle 실패 후 재시도 소진 → DLQ 전송 → commit 호출")
        void handle_fails_sendsToDlq_thenCommits() {
            when(handler.handle(any(), any())).thenReturn(Mono.error(new RuntimeException("처리 실패")));
            stubDlqSuccess();

            StepVerifier.create(registrar.processMessage(record, handler, DLQ_TOPIC))
                    .verifyComplete();

            verify(kafkaSender).send(any());
            verify(receiverOffset).commit();
        }

        @Test
        @DisplayName("handle 실패 + DLQ 전송 실패 → commit 여전히 호출")
        void handle_fails_dlqFails_commitStillCalled() {
            when(handler.handle(any(), any())).thenReturn(Mono.error(new RuntimeException("처리 실패")));
            when(kafkaSender.send(any())).thenReturn(Flux.error(new RuntimeException("DLQ 전송 실패")));

            StepVerifier.create(registrar.processMessage(record, handler, DLQ_TOPIC))
                    .verifyComplete();

            verify(receiverOffset).commit();
        }

        @Test
        @DisplayName("commit 실패 → Mono.empty() 반환 (스트림 종료 안 됨)")
        void commit_fails_completesNormally() {
            when(handler.handle(any(), any())).thenReturn(Mono.empty());
            when(receiverOffset.commit()).thenReturn(Mono.error(new RuntimeException("commit 실패")));

            StepVerifier.create(registrar.processMessage(record, handler, DLQ_TOPIC))
                    .verifyComplete();
        }

        @Test
        @DisplayName("handle 실패 시 DLQ 헤더에 원인 메시지 포함")
        @SuppressWarnings("unchecked")
        void handle_fails_dlqHeader_containsCauseMessage() {
            String errorMessage = "의도된 처리 실패";
            when(handler.handle(any(), any())).thenReturn(Mono.error(new RuntimeException(errorMessage)));

            SenderResult<String> senderResult = mock(SenderResult.class);
            when(senderResult.exception()).thenReturn(null);

            doAnswer(invocation -> {
                Flux.from((org.reactivestreams.Publisher<reactor.kafka.sender.SenderRecord<String, AlertMessage, String>>) invocation.getArgument(0))
                        .doOnNext(sr -> {
                            String headerValue = new String(
                                    sr.headers().lastHeader("x-orig-exception-message").value());
                            assertThat(headerValue).isEqualTo(errorMessage);
                        })
                        .subscribe();
                return Flux.just(senderResult);
            }).when(kafkaSender).send(any());

            StepVerifier.create(registrar.processMessage(record, handler, DLQ_TOPIC))
                    .verifyComplete();
        }
    }

    // ─── helpers ─────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void stubKafkaReceiver(MockedStatic<KafkaReceiver> mocked) {
        KafkaReceiver<String, AlertMessage> mockReceiver = mock(KafkaReceiver.class);
        mocked.when(() -> KafkaReceiver.create(any())).thenReturn(mockReceiver);
        when(mockReceiver.receive()).thenReturn(Flux.never());
    }

    @SuppressWarnings("unchecked")
    private void stubDlqSuccess() {
        SenderResult<String> senderResult = mock(SenderResult.class);
        when(senderResult.exception()).thenReturn(null);
        doReturn(Flux.just(senderResult)).when(kafkaSender).send(any());
    }
}

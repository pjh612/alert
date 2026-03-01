package com.alert.infra.kafka;

import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveKafkaAlertMessagePublisherTest {

    @Mock
    ReactiveAlertCacheManager alertCacheManager;
    @Mock
    AlertMessageSupport support;
    @Mock
    KafkaSender<String, AlertMessage> kafkaSender;
    @Mock
    PartitionKeyStrategy partitionKeyStrategy;

    ReactiveKafkaAlertMessagePublisher publisher;

    private static final String TOPIC = "test-topic";
    private static final String NAMESPACE = "test-ns";
    private static final String MSG_ID = "1700000000001";

    @BeforeEach
    void setUp() {
        publisher = new ReactiveKafkaAlertMessagePublisher(
                alertCacheManager, support, kafkaSender, partitionKeyStrategy);

        when(support.resolveCacheKey(any(), any())).thenReturn("cache-key");
        when(partitionKeyStrategy.resolve(any())).thenReturn("partition-key");
    }

    private AlertMessage createMessage() {
        return new DefaultAlertMessage(
                MSG_ID, NAMESPACE, List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE, "test body", false, null);
    }

    @Test
    @DisplayName("publish: 성공 시 완료")
    void publish_success_completes() {
        AlertMessage msg = createMessage();
        SenderResult<String> senderResult = mock(SenderResult.class);
        when(senderResult.exception()).thenReturn(null);
        doReturn(Flux.just(senderResult)).when(kafkaSender).send(any());

        StepVerifier.create(publisher.publish(TOPIC, msg))
                .verifyComplete();

        verify(kafkaSender).send(any());
    }

    @Test
    @DisplayName("sendToKafka: Kafka 전송 중 예외 발생 시 에러 전파")
    void sendToKafka_kafkaSendException_propagatesError() {
        AlertMessage msg = createMessage();
        doReturn(Flux.error(new RuntimeException("Kafka connection failed"))).when(kafkaSender).send(any());

        StepVerifier.create(publisher.publish(TOPIC, msg))
                .verifyErrorMatches(e -> e.getMessage().contains("Kafka connection failed"));

        verify(kafkaSender).send(any());
    }

    @Test
    @DisplayName("sendToKafka: result.exception() != null 시 에러 전파")
    void sendToKafka_resultHasException_propagatesError() {
        AlertMessage msg = createMessage();
        SenderResult<String> senderResult = mock(SenderResult.class);
        IllegalStateException kafkaException = new IllegalStateException("Kafka send failed");
        when(senderResult.exception()).thenReturn(kafkaException);
        doReturn(Flux.just(senderResult)).when(kafkaSender).send(any());

        StepVerifier.create(publisher.publish(TOPIC, msg))
                .verifyError(IllegalStateException.class);

        verify(kafkaSender).send(any());
    }

    @Test
    @DisplayName("publish: non-cacheable 메시지는 캐시 저장 스킵")
    void publish_nonCacheable_skipsCacheSave() {
        AlertMessage msg = new DefaultAlertMessage(
                MSG_ID, NAMESPACE, List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.CONNECT, "connect body", false, null);
        SenderResult<String> senderResult = mock(SenderResult.class);
        when(senderResult.exception()).thenReturn(null);
        doReturn(Flux.just(senderResult)).when(kafkaSender).send(any());

        StepVerifier.create(publisher.publish(TOPIC, msg))
                .verifyComplete();

        verify(alertCacheManager, never()).save(any(), any(), any());
    }
}

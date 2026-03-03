package com.alert.infra.kafka;

import com.alert.core.cache.AlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaMessagePublisherTest {

    @Mock
    KafkaTemplate<String, AlertMessage> kafkaTemplate;
    @Mock
    AlertCacheManager alertCacheManager;
    @Mock
    AlertMessageSupport support;

    KafkaMessagePublisher publisher;

    private static final String TOPIC = "test-topic";
    private static final String NAMESPACE = "test-ns";
    private static final String MSG_ID = "1700000000001";

    @BeforeEach
    void setUp() {
        publisher = new KafkaMessagePublisher(alertCacheManager, support, kafkaTemplate, PartitionKeyStrategy.none());
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    private AlertMessage createMessage() {
        return new DefaultAlertMessage(
                MSG_ID, NAMESPACE, List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE, "test body", false, null);
    }

    @Test
    @DisplayName("publish: KafkaTemplate.send 호출")
    void publish_sendsToKafka() {
        AlertMessage msg = createMessage();

        publisher.publish(TOPIC, msg);

        verify(kafkaTemplate).send(TOPIC, null, msg);
    }

    @Test
    @DisplayName("publish: 토픽과 메시지 확인")
    void publish_verifiesTopicAndMessage() {
        AlertMessage msg = createMessage();
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<AlertMessage> msgCaptor = ArgumentCaptor.forClass(AlertMessage.class);

        publisher.publish(TOPIC, msg);

        verify(kafkaTemplate).send(topicCaptor.capture(), eq(null), msgCaptor.capture());
        assertThat(topicCaptor.getValue()).isEqualTo(TOPIC);
        assertThat(msgCaptor.getValue()).isEqualTo(msg);
    }
}

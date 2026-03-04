package com.alert.infra.kafka;

import com.alert.core.messaging.consumer.TopicAlertMessageHandler;
import com.alert.core.messaging.model.AlertMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaMessageConsumerRegistrarTest {

    @Mock
    private ConcurrentKafkaListenerContainerFactory<String, AlertMessage> factory;
    @Mock
    private GenericApplicationContext context;
    @Mock
    private CommonErrorHandler errorHandler;
    @Mock
    private ConcurrentMessageListenerContainer<String, AlertMessage> container;
    @Mock
    private ContainerProperties containerProperties;

    private KafkaMessageConsumerRegistrar registrar;

    private final String TOPIC = "test-topic";

    @BeforeEach
    void setUp() {
        registrar = new KafkaMessageConsumerRegistrar(factory, context, errorHandler);

        // 가짜 컨테이너 설정 연결
        when(factory.createContainer(TOPIC)).thenReturn(container);
        when(container.getContainerProperties()).thenReturn(containerProperties);
    }

    @Test
    @DisplayName("register: 정상 등록 시 컨테이너 설정 및 Bean 등록이 수행되어야 함")
    void register_normalTopic_setsUpContainerAndRegistersBean() {
        // Given
        TopicAlertMessageHandler handler = mock(TopicAlertMessageHandler.class);
        String beanName = "alertListenerContainer-" + TOPIC;
        when(context.containsBean(beanName)).thenReturn(false);

        // When
        registrar.register(TOPIC, handler);

        // Then
        // 1. 에러 핸들러 설정 확인
        verify(container).setCommonErrorHandler(errorHandler);

        // 2. 메시지 리스너 설정 확인
        verify(containerProperties).setMessageListener(any(MessageListener.class));

        // 3. Bean 등록 확인 (ArgumentCaptor를 통해 실제 supplier가 container를 반환하는지 검증 가능)
        verify(context).registerBean(eq(beanName), eq(ConcurrentMessageListenerContainer.class), any(Supplier.class));
    }

    @Test
    @DisplayName("register: 에러 핸들러가 null인 경우 설정을 건너뛰어야 함")
    void register_nullErrorHandler_skipsErrorHandlerAssignment() {
        // Given
        registrar = new KafkaMessageConsumerRegistrar(factory, context, null);
        TopicAlertMessageHandler handler = mock(TopicAlertMessageHandler.class);

        // When
        registrar.register(TOPIC, handler);

        // Then
        verify(container, never()).setCommonErrorHandler(any());
    }

    @Test
    @DisplayName("register: 이미 동일한 토픽의 Bean이 존재하면 중복 등록하지 않아야 함")
    void register_duplicateTopic_skipsBeanRegistration() {
        // Given
        TopicAlertMessageHandler handler = mock(TopicAlertMessageHandler.class);
        String beanName = "alertListenerContainer-" + TOPIC;
        when(context.containsBean(beanName)).thenReturn(true);

        // When
        registrar.register(TOPIC, handler);

        // Then
        verify(context, never()).registerBean(anyString(), any(), any(Supplier.class));
    }

    @Test
    @DisplayName("listener: 리스너가 호출되었을 때 핸들러의 handle 메서드를 실행해야 함")
    @SuppressWarnings("unchecked")
    void listener_onMessage_callsMessageHandler() {
        // Given
        TopicAlertMessageHandler handler = mock(TopicAlertMessageHandler.class);
        ArgumentCaptor<MessageListener<String, AlertMessage>> listenerCaptor =
                ArgumentCaptor.forClass(MessageListener.class);

        registrar.register(TOPIC, handler);
        verify(containerProperties).setMessageListener(listenerCaptor.capture());

        MessageListener<String, AlertMessage> listener = listenerCaptor.getValue();
        ConsumerRecord<String, AlertMessage> record = mock(ConsumerRecord.class);
        AlertMessage alertMessage = mock(AlertMessage.class);

        when(record.topic()).thenReturn(TOPIC);
        when(record.value()).thenReturn(alertMessage);

        // When
        listener.onMessage(record);

        // Then
        verify(handler).handle(TOPIC, alertMessage);
    }
}
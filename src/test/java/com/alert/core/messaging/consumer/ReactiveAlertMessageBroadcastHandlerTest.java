package com.alert.core.messaging.consumer;

import com.alert.core.messaging.broadcaster.ReactiveMessageBroadcaster;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.BasicAlertMessageSender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReactiveAlertMessageBroadcastHandlerTest {

    @Mock
    private ReactiveMessageBroadcaster<String, String> messageBroadcaster;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private BasicAlertMessageSender basicSender;

    private ReactiveAlertMessageBroadcastHandler<String> handler;

    private final String TOPIC = "test-topic";
    private final String NAMESPACE = "test-ns";
    private final String MSG_ID = "msg-123";

    @BeforeEach
    void setUp() {
        handler = new ReactiveAlertMessageBroadcastHandler<>(messageBroadcaster, objectMapper, basicSender);
    }

    @Test
    @DisplayName("handle: 정상 메시지 처리 시 basicSender 호출 및 브로드캐스팅 성공")
    void normalMessage_allTargets_callsSenderAndBroadcaster() {
        // Given
        AlertMessage message = mock(AlertMessage.class);
        when(message.namespace()).thenReturn(NAMESPACE);
        when(message.id()).thenReturn(MSG_ID);
        String json = "{\"id\":\"msg-123\"}";

        when(objectMapper.writeValueAsString(message)).thenReturn(json);
        when(messageBroadcaster.sendMessage(TOPIC, json)).thenReturn(Mono.just("SUCCESS"));

        // When
        Mono<String> result = handler.handle(TOPIC, message);

        // Then
        StepVerifier.create(result)
                .expectNext("SUCCESS")
                .verifyComplete();

        verify(basicSender).send(NAMESPACE, MSG_ID, message);
        verify(messageBroadcaster).sendMessage(TOPIC, json);
    }

    @Test
    @DisplayName("handle: basicSender가 null인 경우에도 브로드캐스팅은 정상 작동해야 함")
    void normalMessage_nullBasicSender_callsOnlyBroadcaster() {
        // Given
        handler = new ReactiveAlertMessageBroadcastHandler<>(messageBroadcaster, objectMapper, null);
        AlertMessage message = mock(AlertMessage.class);
        String json = "{\"id\":\"msg-123\"}";

        when(objectMapper.writeValueAsString(message)).thenReturn(json);
        when(messageBroadcaster.sendMessage(TOPIC, json)).thenReturn(Mono.just("SUCCESS"));

        // When
        Mono<String> result = handler.handle(TOPIC, message);

        // Then
        StepVerifier.create(result)
                .expectNext("SUCCESS")
                .verifyComplete();

        verifyNoInteractions(basicSender);
        verify(messageBroadcaster).sendMessage(TOPIC, json);
    }

    @Test
    @DisplayName("handle: JSON 직렬화 에러 발생 시 에러 신호를 반환해야 함")
    void normalMessage_jsonError_returnsErrorMono() {
        // Given
        AlertMessage message = mock(AlertMessage.class);
        doThrow(new RuntimeException("JSON Serialization Failed"))
                .when(objectMapper).writeValueAsString(any());

        // When
        Mono<String> result = handler.handle(TOPIC, message);

        // Then
        StepVerifier.create(result)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    @DisplayName("handle: 브로드캐스터 에러 시 doOnError 로그를 남기고 에러를 전파해야 함")
    void normalMessage_broadcasterError_propagatesError() {
        // Given
        AlertMessage message = mock(AlertMessage.class);
        String json = "{\"id\":\"msg-123\"}";
        when(objectMapper.writeValueAsString(message)).thenReturn(json);
        when(messageBroadcaster.sendMessage(anyString(), anyString()))
                .thenReturn(Mono.error(new RuntimeException("Broadcaster Fail")));

        // When
        Mono<String> result = handler.handle(TOPIC, message);

        // Then
        StepVerifier.create(result)
                .expectError(RuntimeException.class)
                .verify();
    }
}
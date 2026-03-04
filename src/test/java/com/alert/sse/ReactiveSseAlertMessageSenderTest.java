package com.alert.sse;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageType;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReactiveSseAlertMessageSenderTest {

    @Mock
    Sinks.Many<ServerSentEvent<Object>> engine;

    @InjectMocks
    ReactiveSseAlertMessageSender sender;

    @Test
    @DisplayName("doSend 호출 시 ServerSentEvent 객체가 올바른 데이터와 함께 Emit 되어야 한다")
    @SuppressWarnings("unchecked")
    void doSend_ShouldEmitCorrectServerSentEvent() {
        // 1. Given
        String id = "event-999";
        AlertMessage message = mock(AlertMessage.class);
        when(message.type()).thenReturn(DefaultAlertMessageType.MESSAGE);
        when(message.body()).thenReturn("Hello Reactive SSE");

        // 성공 결과 반환 설정
        when(engine.tryEmitNext(any())).thenReturn(Sinks.EmitResult.OK);

        ArgumentCaptor<ServerSentEvent<Object>> captor = ArgumentCaptor.forClass(ServerSentEvent.class);

        // 2. When
        sender.doSend(engine, id, message);

        // 3. Then
        verify(engine).tryEmitNext(captor.capture());
        ServerSentEvent<Object> emittedEvent = captor.getValue();

        assertEquals(id, emittedEvent.id());
        assertEquals(DefaultAlertMessageType.MESSAGE.toString(), emittedEvent.event());
        assertEquals("Hello Reactive SSE", emittedEvent.data());
    }

    @Test
    @DisplayName("Emit 실패 시 에러가 발생하지 않고 로그만 남기고 종료되어야 한다")
    void doSend_WhenEmitFails_ShouldNotThrowException() {
        // 1. Given
        AlertMessage message = mock(AlertMessage.class);
        when(message.type()).thenReturn(DefaultAlertMessageType.MESSAGE);

        // 실패 결과 반환 설정
        when(engine.tryEmitNext(any())).thenReturn(Sinks.EmitResult.FAIL_OVERFLOW);

        // 2. When & Then
        sender.doSend(engine, "fail-id", message);

        verify(engine, times(1)).tryEmitNext(any());
    }
}
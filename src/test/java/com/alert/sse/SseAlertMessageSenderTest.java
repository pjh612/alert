package com.alert.sse;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SseAlertMessageSenderTest {

    @Mock
    TagBasedAlertSessionRepository<SseEmitter> repository;

    @Mock
    SseEmitter emitter;

    @InjectMocks
    SseAlertMessageSender sender;

    @Test
    @DisplayName("doSend 성공 시 SseEmitter의 send 메서드가 호출되어야 한다")
    void doSend_Success() throws IOException {
        // 1. Given
        String messageId = "msg-123";
        AlertMessage message = mock(AlertMessage.class);
        when(message.type()).thenReturn(DefaultAlertMessageType.MESSAGE);
        when(message.body()).thenReturn("Order Data");

        // 2. When (doSend는 protected이므로 직접 호출하거나 send를 통해 호출)
        sender.doSend(emitter, messageId, message);

        ArgumentCaptor<Set<ResponseBodyEmitter.DataWithMediaType>> captor = ArgumentCaptor.forClass(Set.class);

        // 3. Then: ArgumentCaptor로 어떤 Event가 빌드되어 전달됐는지 확인
        verify(emitter).send(captor.capture());
        verify(emitter, never()).completeWithError(any());
        Set<ResponseBodyEmitter.DataWithMediaType> emittedEvent = captor.getValue();

        Assertions.assertThat(emittedEvent.isEmpty()).isFalse();
    }

    @Test
    @DisplayName("send 호출 시 예외가 발생하면 completeWithError가 호출되어야 한다")
    void doSend_Failure_ShouldCompleteWithError() throws IOException {
        // 1. Given
        String messageId = "msg-123";
        AlertMessage message = mock(AlertMessage.class);
        when(message.type()).thenReturn(DefaultAlertMessageType.MESSAGE);

        // 에러 상황 시뮬레이션: emitter.send 호출 시 IOException 발생
        doThrow(new IOException("Connection Closed")).when(emitter).send(any(Set.class));

        // 2. When
        sender.doSend(emitter, messageId, message);

        // 3. Then
        verify(emitter).completeWithError(any(IOException.class));
    }
}
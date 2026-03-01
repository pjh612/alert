package com.alert.core.messaging.consumer;

import com.alert.core.messaging.broadcaster.MessageBroadcaster;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageType;
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
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AlertMessageBroadcastHandlerTest {

    @Mock
    MessageBroadcaster<String> messageBroadcaster;
    @Mock
    ObjectMapper objectMapper;

    AlertMessageBroadcastHandler handler;

    private static final String TOPIC = "test-topic";
    private static final String NAMESPACE = "test-ns";
    private static final String MSG_ID = "1700000000001";

    @BeforeEach
    void setUp() {
        handler = new AlertMessageBroadcastHandler(messageBroadcaster, objectMapper);
    }

    private AlertMessage createMessage() {
        return new DefaultAlertMessage(
                MSG_ID, NAMESPACE, List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE, "test body", false, null);
    }

    @Test
    @DisplayName("handle: 메시지를 JSON으로 변환 후 브로드캐스터로 전송")
    void handle_sendsToBroadcaster() throws Exception {
        AlertMessage msg = createMessage();
        when(objectMapper.writeValueAsString(msg)).thenReturn("{\"id\":\"1700000000001\"}");

        handler.handle(TOPIC, msg);

        verify(messageBroadcaster).sendMessage(eq(TOPIC), anyString());
    }

    @Test
    @DisplayName("handle: 변환된 JSON 문자열 확인")
    void handle_verifiesJsonContent() throws Exception {
        AlertMessage msg = createMessage();
        String json = "{\"id\":\"1700000000001\",\"body\":\"test body\"}";
        when(objectMapper.writeValueAsString(msg)).thenReturn(json);
        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);

        handler.handle(TOPIC, msg);

        verify(messageBroadcaster).sendMessage(eq(TOPIC), jsonCaptor.capture());
        assertThat(jsonCaptor.getValue()).isEqualTo(json);
    }

    @Test
    @DisplayName("handle: JSON 변환 예외 발생 시 로그 출력")
    void handle_jsonException_logsError() throws Exception {
        AlertMessage msg = createMessage();
        when(objectMapper.writeValueAsString(msg)).thenThrow(new RuntimeException("json error"));

        handler.handle(TOPIC, msg);

        verify(messageBroadcaster, never()).sendMessage(anyString(), anyString());
    }
}

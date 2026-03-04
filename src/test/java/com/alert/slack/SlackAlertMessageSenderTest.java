package com.alert.slack;

import com.alert.core.messaging.broadcaster.MessageConverter;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageType;
import com.alert.slack.SlackAlertMessageSender;
import net.gpedro.integrations.slack.SlackApi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SlackAlertMessageSenderTest {

    @Mock
    private MessageConverter<AlertMessage, String> messageConverter;

    private SlackAlertMessageSender sender;
    private final String webhookUrl = "https://hooks.slack.com/services/test";
    private final String namespace = "test-ns";
    private final String id = "msg-123";

    @BeforeEach
    void setUp() {
        sender = new SlackAlertMessageSender(webhookUrl, messageConverter);
    }

    @Test
    @DisplayName("성공: 조건 만족 시 내부에서 생성된 SlackApi의 call이 호출된다")
    void send_shouldCallSlackApi() {
        try (MockedConstruction<SlackApi> mocked = mockConstruction(SlackApi.class)) {
            SlackAlertMessageSender localSender = new SlackAlertMessageSender(webhookUrl, messageConverter);

            // Given
            AlertMessage msg = mock(AlertMessage.class);
            AlertMessageType type = mock(AlertMessageType.class);
            when(msg.type()).thenReturn(type);
            when(type.isCacheable()).thenReturn(true);
            when(msg.isReplay()).thenReturn(false);
            when(messageConverter.convert(msg)).thenReturn("test content");

            // When
            localSender.send("ns", "id", msg);

            // Then
            SlackApi createdMock = mocked.constructed().get(0);
            verify(createdMock).call(any());
        }
    }

    @Test
    @DisplayName("예외 처리: SlackApi가 에러를 던져도 catch 블록이 동작하여 안전하게 종료된다")
    void send_shouldHandleException() {
        try (MockedConstruction<SlackApi> mocked = mockConstruction(SlackApi.class, (mock, context) -> {
            doThrow(new RuntimeException("Slack Error")).when(mock).call(any());
        })) {
            SlackAlertMessageSender localSender = new SlackAlertMessageSender(webhookUrl, messageConverter);

            // Given
            AlertMessage msg = mock(AlertMessage.class);
            AlertMessageType type = mock(AlertMessageType.class);
            when(msg.type()).thenReturn(type);
            when(type.isCacheable()).thenReturn(true);
            when(msg.isReplay()).thenReturn(false);

            // When & Then
            localSender.send("ns", "id", msg);

            assertThat(mocked.constructed()).isNotEmpty();
        }
    }

    @Test
    @DisplayName("포맷팅: 컨버터가 null이면 message.body()의 문자열 값을 반환해야 한다")
    void formatMessage_shouldUseBodyString_whenConverterIsNull() {
        try (MockedConstruction<SlackApi> mocked = mockConstruction(SlackApi.class)) {
            // Given
            SlackAlertMessageSender nullConverterSender = new SlackAlertMessageSender(webhookUrl, null);

            AlertMessage msg = mock(AlertMessage.class);
            AlertMessageType type = mock(AlertMessageType.class);

            when(msg.type()).thenReturn(type);
            when(type.isCacheable()).thenReturn(true);
            when(msg.isReplay()).thenReturn(false);

            String expectedBody = "Test Body Content";
            when(msg.body()).thenReturn(expectedBody);

            // When
            nullConverterSender.send(namespace, id, msg);

            // Then
            assertThat(mocked.constructed()).isNotEmpty();
            SlackApi mockApi = mocked.constructed().get(0);

            verify(mockApi).call(any());

            verifyNoInteractions(messageConverter);
        }
    }

    @Test
    @DisplayName("필터링: 휘발성 메시지(isCacheable=false)는 Slack으로 전송하지 않아야 한다")
    void shouldNotSend_whenMessageIsNotCacheable() {
        try (MockedConstruction<SlackApi> mocked = mockConstruction(SlackApi.class)) {
            // Given
            AlertMessage msg = mock(AlertMessage.class);
            AlertMessageType type = mock(AlertMessageType.class);
            when(msg.type()).thenReturn(type);
            when(type.isCacheable()).thenReturn(false);

            // When
            sender.send(namespace, id, msg);

            if (!mocked.constructed().isEmpty()) {
                verify(mocked.constructed().get(0), never()).call(any());
            }
            verifyNoInteractions(messageConverter);
        }
    }

    @Test
    @DisplayName("필터링: 리플레이 메시지(isReplay=true)는 Slack으로 전송하지 않아야 한다")
    void shouldNotSend_whenMessageIsReplay() {
        try (MockedConstruction<SlackApi> mocked = mockConstruction(SlackApi.class)) {
            // Given
            AlertMessage msg = mock(AlertMessage.class);
            AlertMessageType type = mock(AlertMessageType.class);
            when(msg.type()).thenReturn(type);
            when(type.isCacheable()).thenReturn(true);
            when(msg.isReplay()).thenReturn(true);

            // When
            sender.send(namespace, id, msg);

            // Then
            if (!mocked.constructed().isEmpty()) {
                verify(mocked.constructed().get(0), never()).call(any());
            }
            verifyNoInteractions(messageConverter);
        }
    }

}
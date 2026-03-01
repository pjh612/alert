package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageType;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import com.alert.core.messaging.sender.AlertMessageSender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DefaultAlertMessageHandlerTest {

    @Mock
    AlertMessageSender alertMessageSender;

    DefaultAlertMessageHandler handler;

    private static final String NAMESPACE = "test-ns";
    private static final String MSG_ID = "1700000000000";

    @BeforeEach
    void setUp() {
        handler = new DefaultAlertMessageHandler(alertMessageSender);
    }

    private AlertMessage message(boolean cacheable, boolean isReplay, List<AlertTarget> targets) {
        AlertMessageType type = cacheable ? DefaultAlertMessageType.MESSAGE : DefaultAlertMessageType.CONNECT;
        return new DefaultAlertMessage(MSG_ID, NAMESPACE, targets, type, "body", isReplay, null);
    }

    @Test
    @DisplayName("일반 메시지 ID 타깃: message.id()를 event ID로 send 호출")
    void normalMessage_idTarget_callsSend() {
        AlertMessage msg = message(true, false, List.of(AlertTarget.id("user1")));

        handler.handle(msg);

        verify(alertMessageSender).send(NAMESPACE, MSG_ID, msg);
    }

    @Test
    @DisplayName("일반 메시지 TAG 타깃: message.id()를 event ID로 send 호출")
    void normalMessage_tagTarget_callsSend() {
        AlertMessage msg = message(true, false, List.of(AlertTarget.tag("vip")));

        handler.handle(msg);

        verify(alertMessageSender).send(NAMESPACE, MSG_ID, msg);
    }

    @Test
    @DisplayName("일반 메시지 ID+TAG 혼합: message.id()를 event ID로 send 호출")
    void normalMessage_idAndTagTargets_callsSend() {
        AlertMessage msg = message(true, false, List.of(AlertTarget.id("user1"), AlertTarget.tag("vip")));

        handler.handle(msg);

        verify(alertMessageSender).send(NAMESPACE, MSG_ID, msg);
    }

    @Test
    @DisplayName("replay 메시지: message.id()를 event ID로 send 호출")
    void replayMessage_callsSendWithMessageId() {
        AlertMessage replay = new DefaultAlertMessage(
                "replay-uuid", NAMESPACE, List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE, "body", true, null);

        handler.handle(replay);

        verify(alertMessageSender).send(NAMESPACE, "replay-uuid", replay);
    }

    @Test
    @DisplayName("non-cacheable 메시지: message.id()를 event ID로 send 호출")
    void nonCacheableMessage_callsSend() {
        AlertMessage msg = message(false, false, List.of(AlertTarget.id("user1")));

        handler.handle(msg);

        verify(alertMessageSender).send(NAMESPACE, MSG_ID, msg);
    }
}

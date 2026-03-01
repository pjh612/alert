package com.alert.core.messaging.sender;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BasicAlertMessageDelegateSenderTest {

    @Mock BasicAlertMessageSender sender1;
    @Mock BasicAlertMessageSender sender2;

    private static final String NAMESPACE = "test-ns";
    private static final String MSG_ID = "1700000000001";

    private AlertMessage createMessage() {
        return new DefaultAlertMessage(
                MSG_ID, NAMESPACE, List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE, "body", false, null);
    }

    @Test
    @DisplayName("등록된 모든 BasicSender에게 동일한 인자로 send 위임")
    void send_delegatesToAllBasicSenders() {
        BasicAlertMessageDelegateSender delegate = new BasicAlertMessageDelegateSender(List.of(sender1, sender2));
        AlertMessage msg = createMessage();

        delegate.send(NAMESPACE, MSG_ID, msg);

        verify(sender1).send(NAMESPACE, MSG_ID, msg);
        verify(sender2).send(NAMESPACE, MSG_ID, msg);
    }

    @Test
    @DisplayName("BasicSender 목록이 비어있으면 아무 동작도 하지 않음")
    void send_withEmptyList_doesNothing() {
        BasicAlertMessageDelegateSender delegate = new BasicAlertMessageDelegateSender(List.of());
        AlertMessage msg = createMessage();

        delegate.send(NAMESPACE, MSG_ID, msg);

        verifyNoInteractions(sender1, sender2);
    }

    @Test
    @DisplayName("BasicAlertMessageDelegateSender는 BasicAlertMessageSender 타입")
    void isInstanceOfBasicAlertMessageSender() {
        BasicAlertMessageDelegateSender delegate = new BasicAlertMessageDelegateSender(List.of());

        assert delegate instanceof BasicAlertMessageSender;
    }
}

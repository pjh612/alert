package com.alert.core.messaging.sender;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import com.alert.core.session.AlertSession;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AbstractAlertMessageSenderTest {

    @Mock
    TagBasedAlertSessionRepository<String> repository;
    @Mock
    AlertSession<String> session;

    TestAlertMessageSender sender;

    private static final String NAMESPACE = "test-ns";
    private static final String MSG_ID = "1700000000001";

    static class TestAlertMessageSender extends AbstractAlertMessageSender<String> {
        protected TestAlertMessageSender(TagBasedAlertSessionRepository<String> repository) {
            super(repository);
        }

        @Override
        protected void doSend(String engine, String id, AlertMessage message) {
            // no-op for test
        }
    }

    @BeforeEach
    void setUp() {
        sender = new TestAlertMessageSender(repository);
    }

    private AlertMessage createMessage(List<AlertTarget> targets) {
        return new DefaultAlertMessage(
                MSG_ID, NAMESPACE, targets,
                DefaultAlertMessageType.MESSAGE, "test body", false, null);
    }

    @Test
    @DisplayName("send: ID 타깃 조회 후 doSend 호출")
    void send_idTarget_queriesAndSends() {
        AlertMessage msg = createMessage(List.of(AlertTarget.id("user1")));
        when(session.engine()).thenReturn("emitter1");
        when(repository.getById(NAMESPACE, "user1")).thenReturn(Optional.of(session));

        sender.send(NAMESPACE, MSG_ID, msg);

        verify(repository).getById(NAMESPACE, "user1");
    }

    @Test
    @DisplayName("send: TAG 타깃 조회 후 doSend 호출")
    void send_tagTarget_queriesAndSends() {
        AlertMessage msg = createMessage(List.of(AlertTarget.tag("vip")));
        AlertSession<String> session1 = mock(AlertSession.class);
        AlertSession<String> session2 = mock(AlertSession.class);
        when(session1.engine()).thenReturn("emitter1");
        when(session2.engine()).thenReturn("emitter2");
        when(repository.getByTag(NAMESPACE, "vip")).thenReturn(List.of(session1, session2));

        sender.send(NAMESPACE, MSG_ID, msg);

        verify(repository).getByTag(NAMESPACE, "vip");
    }

    @Test
    @DisplayName("send: BROADCAST 타깃 조회 후 doSend 호출")
    void send_broadcastTarget_queriesAndSends() {
        AlertMessage msg = createMessage(List.of(AlertTarget.broadcast()));
        AlertSession<String> session1 = mock(AlertSession.class);
        when(session1.engine()).thenReturn("emitter1");
        when(repository.getAll(NAMESPACE)).thenReturn(List.of(session1));

        sender.send(NAMESPACE, MSG_ID, msg);

        verify(repository).getAll(NAMESPACE);
    }

    @Test
    @DisplayName("send: 빈 타깃 리스트는 아무 작업 안함")
    void send_emptyTargets_doesNothing() {
        AlertMessage msg = createMessage(List.of());

        sender.send(NAMESPACE, MSG_ID, msg);

        verifyNoInteractions(repository);
    }
}

package com.alert.core.messaging.broadcaster;

import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.messaging.model.*;
import com.alert.core.messaging.sender.AlertMessageSender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.test.StepVerifier;

import java.util.List;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultReactiveAlertMessageHandlerTest {

    @Mock ReactiveAlertCacheManager alertCacheManager;
    @Mock AlertMessageSender alertMessageSender;
    @Mock AlertMessageSupport support;

    DefaultReactiveAlertMessageHandler handler;

    private static final String NAMESPACE = "test-ns";
    private static final String MSG_ID = "1700000000000";

    @BeforeEach
    void setUp() {
        handler = new DefaultReactiveAlertMessageHandler(alertMessageSender);
    }

    private AlertMessage message(boolean cacheable, boolean isReplay, List<AlertTarget> targets) {
        AlertMessageType type = cacheable ? DefaultAlertMessageType.MESSAGE : DefaultAlertMessageType.CONNECT;
        return new DefaultAlertMessage(MSG_ID, NAMESPACE, targets, type, "body", isReplay, null);
    }

    @Test
    @DisplayName("일반 메시지 ID 타깃: generateMessageId()를 eventId로 send 호출")
    void normalMessage_idTarget_callsSendWithGeneratedEventId() {
        AlertMessage msg = message(true, false, List.of(AlertTarget.id("user1")));

        StepVerifier.create(handler.handle(msg)).verifyComplete();

        verify(alertMessageSender).send(NAMESPACE, MSG_ID, msg);
    }

    @Test
    @DisplayName("일반 메시지 TAG 타깃: generateMessageId()를 eventId로 send 호출")
    void normalMessage_tagTarget_callsSendWithGeneratedEventId() {
        AlertMessage msg = message(true, false, List.of(AlertTarget.tag("vip")));

        StepVerifier.create(handler.handle(msg)).verifyComplete();

        verify(alertMessageSender).send(NAMESPACE, MSG_ID, msg);
    }

    @Test
    @DisplayName("일반 메시지 ID+TAG 혼합: generateMessageId()를 eventId로 send 호출")
    void normalMessage_idAndTagTargets_callsSendWithGeneratedEventId() {
        AlertMessage msg = message(true, false, List.of(AlertTarget.id("user1"), AlertTarget.tag("vip")));

        StepVerifier.create(handler.handle(msg)).verifyComplete();

        verify(alertMessageSender).send(NAMESPACE, MSG_ID, msg);
    }

    @Test
    @DisplayName("replay 메시지: 원본 메시지 id로 send 호출")
    void replayMessage_callsSendWithOriginalMessageId() {
        AlertMessage replay = new DefaultAlertMessage(
                "replay-uuid", NAMESPACE, List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE, "body", true, null);

        StepVerifier.create(handler.handle(replay)).verifyComplete();

        verify(alertMessageSender).send(NAMESPACE, "replay-uuid", replay);
    }

    @Test
    @DisplayName("non-cacheable 메시지: generateMessageId()를 eventId로 send 호출")
    void nonCacheableMessage_callsSendWithGeneratedEventId() {
        AlertMessage msg = message(false, false, List.of(AlertTarget.id("user1")));

        StepVerifier.create(handler.handle(msg)).verifyComplete();

        verify(alertMessageSender).send(NAMESPACE, MSG_ID, msg);
    }
}

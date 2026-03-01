package com.alert.core.manager;

import com.alert.core.messaging.model.*;
import com.alert.core.messaging.publisher.AlertMessagePublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AbstractAlertManagerTest {

    @Mock
    AlertMessagePublisher alertMessagePublisher;
    @Mock
    AlertMessageFactory alertMessageFactory;

    TestAlertManager manager;

    static class TestAlertManager extends AbstractAlertManager {
        protected TestAlertManager(AlertMessageFactory factory, AlertMessagePublisher publisher) {
            super(factory, publisher);
        }
    }

    private static final String NAMESPACE = "test-ns";
    private static final String TOPIC = "test-ns";

    @BeforeEach
    void setUp() {
        AlertChannel channel = () -> NAMESPACE;
        manager = new TestAlertManager(alertMessageFactory, alertMessagePublisher);
    }

    @Test
    @DisplayName("notice: ID 타깃으로 알림 전송")
    void notice_byId_createsAndPublishesMessage() {
        AlertChannel channel = () -> TOPIC;
        when(alertMessageFactory.create(eq(NAMESPACE), eq(List.of(AlertTarget.id("user1"))), eq(DefaultAlertMessageType.MESSAGE), eq("Hello"), isNull()))
                .thenReturn(mock(AlertMessage.class));

        manager.notice(channel, "user1", "Hello");

        verify(alertMessageFactory).create(eq(NAMESPACE), eq(List.of(AlertTarget.id("user1"))), eq(DefaultAlertMessageType.MESSAGE), eq("Hello"), isNull());
        verify(alertMessagePublisher).publish(eq(TOPIC), any(AlertMessage.class));
    }

    @Test
    @DisplayName("noticeByTag: 태그 타깃으로 알림 전송")
    void noticeByTag_createsAndPublishesMessage() {
        AlertChannel channel = () -> TOPIC;
        when(alertMessageFactory.create(eq(NAMESPACE), eq(List.of(AlertTarget.tag("vip"))), eq(DefaultAlertMessageType.MESSAGE), eq("VIP alert"), isNull()))
                .thenReturn(mock(AlertMessage.class));

        manager.noticeByTag(channel, "vip", "VIP alert");

        verify(alertMessageFactory).create(eq(NAMESPACE), eq(List.of(AlertTarget.tag("vip"))), eq(DefaultAlertMessageType.MESSAGE), eq("VIP alert"), isNull());
        verify(alertMessagePublisher).publish(eq(TOPIC), any(AlertMessage.class));
    }

    @Test
    @DisplayName("broadcast: 전체 브로드캐스트")
    void broadcast_createsAndPublishesMessage() {
        AlertChannel channel = () -> TOPIC;
        when(alertMessageFactory.create(eq(NAMESPACE), eq(List.of(AlertTarget.broadcast())), eq(DefaultAlertMessageType.MESSAGE), eq("Broadcast msg"), isNull()))
                .thenReturn(mock(AlertMessage.class));

        manager.broadcast(channel, "Broadcast msg");

        verify(alertMessageFactory).create(eq(NAMESPACE), eq(List.of(AlertTarget.broadcast())), eq(DefaultAlertMessageType.MESSAGE), eq("Broadcast msg"), isNull());
        verify(alertMessagePublisher).publish(eq(TOPIC), any(AlertMessage.class));
    }

    @Test
    @DisplayName("notice: 복수 타겟으로 알림 전송")
    void notice_multipleTargets_createsAndPublishesMessage() {
        AlertChannel channel = () -> TOPIC;
        List<AlertTarget> targets = List.of(AlertTarget.id("user1"), AlertTarget.tag("vip"));
        when(alertMessageFactory.create(eq(NAMESPACE), eq(targets), eq(DefaultAlertMessageType.MESSAGE), eq("Multi target"), isNull()))
                .thenReturn(mock(AlertMessage.class));

        manager.notice(channel, targets, "Multi target");

        verify(alertMessageFactory).create(eq(NAMESPACE), eq(targets), eq(DefaultAlertMessageType.MESSAGE), eq("Multi target"), isNull());
        verify(alertMessagePublisher).publish(eq(TOPIC), any(AlertMessage.class));
    }
}

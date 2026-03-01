package com.alert.core.messaging.model;

import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultAlertMessageFactoryTest {

    @Mock
    AlertMessageSupport support;

    private DefaultAlertMessageFactory factory;

    @BeforeEach
    void setUp() {
        factory = new DefaultAlertMessageFactory(support);
        when(support.generateMessageId()).thenReturn("generated-id");
    }

    @Test
    @DisplayName("onConnect: 연결 메시지 생성")
    void onConnect_createsMessage() {
        AlertMessage msg = factory.onConnect("test-ns", "user1", Map.of("key", "value"));

        assertThat(msg.id()).isEqualTo("generated-id");
        assertThat(msg.namespace()).isEqualTo("test-ns");
        assertThat(msg.targets()).containsExactly(AlertTarget.id("user1"));
        assertThat(msg.type()).isEqualTo(DefaultAlertMessageType.CONNECT);
        assertThat(msg.body()).isEqualTo("connected");
        assertThat(msg.isReplay()).isFalse();
        assertThat(msg.attributes()).containsEntry("key", "value");
    }

    @Test
    @DisplayName("onReplay: 재전송 메시지 생성")
    void onReplay_createsMessage() {
        AlertMessage original = new DefaultAlertMessage(
                "100", "test-ns", List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE, "original body", false, null);

        AlertMessage replay = factory.onReplay("test-ns", "user1", original, null);

        assertThat(replay.id()).isEqualTo("generated-id");
        assertThat(replay.namespace()).isEqualTo("test-ns");
        assertThat(replay.targets()).containsExactly(AlertTarget.id("user1"));
        assertThat(replay.type()).isEqualTo(DefaultAlertMessageType.MESSAGE);
        assertThat(replay.body()).isEqualTo("original body");
        assertThat(replay.isReplay()).isTrue();
    }

    @Test
    @DisplayName("create: 일반 메시지 생성")
    void create_createsMessage() {
        List<AlertTarget> targets = List.of(AlertTarget.id("user1"), AlertTarget.tag("vip"));
        AlertMessage msg = factory.create("test-ns", targets, DefaultAlertMessageType.MESSAGE, "hello", null);

        assertThat(msg.id()).isEqualTo("generated-id");
        assertThat(msg.namespace()).isEqualTo("test-ns");
        assertThat(msg.targets()).containsExactlyInAnyOrderElementsOf(targets);
        assertThat(msg.type()).isEqualTo(DefaultAlertMessageType.MESSAGE);
        assertThat(msg.body()).isEqualTo("hello");
        assertThat(msg.isReplay()).isFalse();
    }
}

package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.id.IdGenerator;
import com.alert.core.messaging.model.AlertTarget;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AlertMessageSupportTest {

    @Mock
    IdGenerator<Long> idGenerator;

    AlertMessageSupport support;

    @BeforeEach
    void setUp() {
        support = new AlertMessageSupport(idGenerator);
    }

    @Test
    @DisplayName("generateMessageId: IdGenerator를 통해 ID 생성")
    void generateMessageId_returnsIdFromGenerator() {
        when(idGenerator.nextId()).thenReturn(1700000000001L);

        String id = support.generateMessageId();

        assertThat(id).isEqualTo("1700000000001");
    }

    @Test
    @DisplayName("resolveCacheKey: ID 타깃의 캐시 키 생성")
    void resolveCacheKey_idTarget_returnsCorrectKey() {
        AlertTarget target = AlertTarget.id("user1");

        String key = support.resolveCacheKey("test-ns", target);

        assertThat(key).isEqualTo("alert:test-ns:user:user1");
    }

    @Test
    @DisplayName("resolveCacheKey: TAG 타깃의 캐시 키 생성")
    void resolveCacheKey_tagTarget_returnsCorrectKey() {
        AlertTarget target = AlertTarget.tag("vip");

        String key = support.resolveCacheKey("test-ns", target);

        assertThat(key).isEqualTo("alert:test-ns:tag:vip");
    }

    @Test
    @DisplayName("resolveOffsetKey: 오프셋 키 생성")
    void resolveOffsetKey_returnsCorrectKey() {
        String key = support.resolveOffsetKey("test-ns", "user1");

        assertThat(key).isEqualTo("alert:offset:test-ns:user1");
    }
}

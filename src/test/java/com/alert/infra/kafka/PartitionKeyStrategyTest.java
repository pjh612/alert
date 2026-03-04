package com.alert.infra.kafka;

import com.alert.core.messaging.model.AlertMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PartitionKeyStrategyTest {
    @Test
    @DisplayName("none: 파티션 키 전략을 none으로 설정하면 null을 반환해야 함")
    void none_anyMessage_returnsNull() {
        // Given
        PartitionKeyStrategy strategy = PartitionKeyStrategy.none();
        AlertMessage message = mock(AlertMessage.class);

        // When
        String result = strategy.resolve(message);

        // Then
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("byNamespace: 메시지의 네임스페이스를 파티션 키로 반환해야 함")
    void byNamespace_messageTarget_returnsNamespaceAsKey() {
        // Given
        PartitionKeyStrategy strategy = PartitionKeyStrategy.byNamespace();
        AlertMessage message = mock(AlertMessage.class);
        when(message.namespace()).thenReturn("order-service");

        // When
        String key = strategy.resolve(message);

        // Then
        assertThat(key).isEqualTo("order-service");
    }
}
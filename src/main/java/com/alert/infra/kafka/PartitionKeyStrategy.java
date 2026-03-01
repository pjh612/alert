package com.alert.infra.kafka;

import com.alert.core.messaging.model.AlertMessage;

/**
 * Kafka 파티션 키 결정 전략.
 *
 * <p>같은 키를 가진 메시지는 동일 파티션에 순서대로 전달된다.
 * {@code null}을 반환하면 Kafka 기본 파티셔너(라운드로빈)가 적용된다.
 *
 * <pre>
 * // namespace 기준 순서 보장
 * {@literal @}Bean
 * public PartitionKeyStrategy partitionKeyStrategy() {
 *     return PartitionKeyStrategy.byNamespace();
 * }
 * </pre>
 */
@FunctionalInterface
public interface PartitionKeyStrategy {

    /**
     * 메시지로부터 파티션 키를 결정한다.
     *
     * @return 파티션 키 문자열, {@code null}이면 기본 파티셔너 적용
     */
    String resolve(AlertMessage message);

    /** 파티션 키를 사용하지 않는다 (Kafka 기본 파티셔너). */
    static PartitionKeyStrategy none() {
        return msg -> null;
    }

    /** {@link AlertMessage#namespace()}를 파티션 키로 사용한다. */
    static PartitionKeyStrategy byNamespace() {
        return AlertMessage::namespace;
    }
}

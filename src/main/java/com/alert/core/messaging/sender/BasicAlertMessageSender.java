package com.alert.core.messaging.sender;

/**
 * 메시지를 정확히 한 번 실행해야 하는 Sender.
 *
 * <p>메시지 소비자(Consumer)가 단일 인스턴스에서만 처리하는 구조에서 사용된다.
 * 세션 조회 없이 단일 호출로 동작해야 하는 Slack, Email 등의 전송 수단에 적합하다.
 *
 * <p>예: Kafka + Redis Pub/Sub 조합이라면, Kafka Consumer 단계에서 호출되며
 * 공유 Group ID로 인해 하나의 인스턴스만 소비하므로 메시지당 정확히 한 번 실행된다.
 *
 * @see FanoutAlertMessageSender
 */
public interface BasicAlertMessageSender extends AlertMessageSender {
}

package com.alert.core.messaging.sender;

/**
 * 모든 인스턴스에 fan-out 방식으로 호출되는 Sender.
 *
 * <p>메시지 브로드캐스터가 전체 인스턴스에 메시지를 전파하고, 각 인스턴스는
 * 세션 저장소를 조회해 해당 세션이 있는 경우에만 실제 전송을 수행한다.
 * 클라이언트 연결이 특정 인스턴스에 고정되는 SSE 같은 전송 수단에 적합하다.
 *
 * <p>예: Kafka + Redis Pub/Sub 조합이라면, Redis Pub/Sub Handler에서
 * 모든 인스턴스에 호출되며 자신의 세션 저장소에 해당 세션이 있을 때만 메시지를 전송한다.
 *
 * @see BasicAlertMessageSender
 */
public interface FanoutAlertMessageSender extends AlertMessageSender {
}

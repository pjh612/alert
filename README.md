
# Alert — 실시간 알림 라이브러리

> **Kafka → Redis Pub/Sub → SSE** 파이프라인을 Spring Boot 자동 설정 한 줄로 구성하는 실시간 알림 라이브러리

JitPack: `com.github.pjh612:alert:1.2.5`

---

## 한눈에 보기

| 특성 | 내용 |
|------|------|
| **메시지 파이프라인** | Kafka (내구성) → Redis Pub/Sub (인스턴스 전파) → SSE (클라이언트 푸시) |
| **두 가지 런타임** | Blocking + Virtual Threads (MVC) / Reactive (WebFlux) — 프로퍼티 전환 |
| **자동 설정** | `@ConditionalOnClass` + `@ConditionalOnProperty`로 classpath 기반 선택적 로딩 |
| **확장 포인트** | 9개 인터페이스 — Bean 교체만으로 브로커·캐시·전송 수단 변경 |
| **Graceful Shutdown** | SmartLifecycle phase 제어 + Reactive drain (`takeUntilOther`) |
| **테스트** | 213 케이스, Testcontainers + Embedded Redis, JaCoCo 92% |

### Quick Start

```yaml
# application.yml
alert:
  namespace: order-service
  group-id: order-alert-group
  topics:
    - order-alert
spring:
  threads:
    virtual:
      enabled: true   # Virtual Threads 활성화 (선택)
```

```java
// 특정 사용자에게 알림
alertManager.notice(channel, userId, payload);

// 태그 기반 그룹 알림 — O(1) 조회
alertManager.noticeByTag(channel, "VIP", payload);

// 전체 브로드캐스트
alertManager.broadcast(channel, payload);

// SSE 구독 (Reactive)
Flux<ServerSentEvent<Object>> stream =
    alertManager.subscribe(channel, userId, tags, lastEventId, timeout);
```

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| 언어 | Java 21+ |
| 프레임워크 | Spring Boot 4.0, Spring WebFlux |
| 메시지 브로커 | Apache Kafka, Reactor Kafka |
| 캐시 / Pub-Sub | Redis (Lettuce) |
| 실시간 통신 | SSE (Server-Sent Events) |
| 외부 알림 | Slack Webhook |
| 테스트 | JUnit 5, Mockito, Testcontainers, Embedded Redis, JaCoCo |

---

## 시스템 아키텍처

```
Application
  └─ alertManager.notice(channel, userId, payload)
       │
       ▼  [AlertMessagePublisher]
  메시지 큐 (Kafka)
       │
       ▼  [MessageConsumerRegistrar]
  TopicAlertMessageHandler
       │
       ├──▶  BasicAlertMessageSender   ── 메시지당 1회 실행 (Slack, Email 등)
       │
       ├── 처리 실패
       │       └──▶ DLQ 토픽             ── 재시도 소진 후 격리
       │
       └──▶  [MessageBroadcaster]
               │
               ▼  전체 인스턴스로 전파 (Redis Pub/Sub)
          [AlertMessageHandler — namespace 기반 라우팅]
               │
               ▼
          AlertSessionRepository
          (namespace → id|tag → session)
               │
               ▼
          FanoutAlertMessageSender   ── 세션 보유 인스턴스에서만 전송 (SSE)
```

**왜 브로커가 두 단계인가?**

처음에는 Kafka에서 바로 SSE로 전송하는 단일 브로커 구조를 고려했습니다. 하지만 SSE 연결은 임의의 인스턴스에 고정되고, Kafka Consumer Group은 파티션 단위로 단일 인스턴스에서만 소비합니다. 즉, userId=1의 SSE가 인스턴스 A에 연결되어 있는데 해당 메시지를 인스턴스 B가 소비하면 전달이 불가능합니다.

이를 해결하기 위해 두 단계로 분리했습니다:
- **1단계 (Kafka)**: 서비스 간 메시지 내구성 보장 + Consumer Group 기반 단일 소비
- **2단계 (Redis Pub/Sub)**: 동일 서비스의 모든 인스턴스에 전파 → 세션 보유 인스턴스가 로컬에서 전송

---

## 설계 결정

### 1. Namespace 기반 세션 격리 — 서비스 간 메시지 교차 방지

**문제 상황**

여러 서비스가 동일한 라이브러리를 사용하는 경우, 세션 저장소를 공유하면 네임스페이스 충돌로 메시지가 잘못 전달될 위험이 있습니다. 예를 들어 ORDER 서비스의 userId=1과 PAYMENT 서비스의 userId=1이 같은 세션으로 취급될 수 있습니다.

**설계 결정**

세션 저장소를 namespace 단위로 파티셔닝했습니다.

```java
// namespace → id → session 이중 맵 구조
Map<String, Map<String, AlertSession<T>>> sessionMap;
Map<String, Map<String, Set<String>>> tagIndex;
```

메시지 라우팅 시 namespace를 기반으로 필터링하므로, 서로 다른 채널의 메시지가 교차 전달되는 것을 구조적으로 방지합니다.

**결과**: 논리적 완전 격리, 보안 위험 제거, 멀티태넌시 지원 가능

---

### 2. Tag 기반 그룹 라우팅 — 세션 전체 순회 없이 그룹 알림

**문제 상황**

"골드 등급 회원 전체"에게 알림을 보낼 때, 태그 인덱스 없이는 해당 그룹에 속한 세션을 찾기 위해 현재 연결된 **세션 전체를 순회**해야 합니다. 동시 접속자가 많을수록 불필요한 탐색 비용이 누적됩니다.

**설계 결정**

구독 시 태그를 함께 등록하고, 세션 저장소 내부에서 태그 인덱스를 별도로 관리합니다.

```java
// 구독 시 태그 등록
alertManager.subscribe(channel, userId, List.of("RANK_GOLD", "REGION_SEOUL"), ...);

// 태그 인덱스로 그룹 세션 직접 조회
public List<AlertSession<T>> getByTag(String namespace, String tag) {
    Set<String> ids = tagIndex.get(namespace).get(tag);
    return ids.stream().map(sessionMap.get(namespace)::get).toList();
}
```

**결과**: 세션 전체 순회 O(전체 세션 수) 대신 태그 인덱스로 O(태그 내 세션 수) 조회

---

### 3. 53비트 Snowflake ID — Redis ZSet score 정밀도 문제 해결

**문제 발견**

메시지 재전송을 위해 Redis ZSet에 메시지를 score(메시지 ID) 기준으로 저장했습니다. 처음에는 표준 Snowflake(63비트)를 사용했으나, 재전송 시 순서가 뒤섞이는 버그를 발견했습니다.

원인을 추적해 보니 **Redis ZSet의 score는 IEEE 754 double 타입(유효 정수 범위 2^53 - 1)**으로 저장됩니다. 63비트 정수를 double로 변환하면 정밀도 손실이 발생해 서로 다른 ID가 같은 score로 저장될 수 있었습니다.

**고민한 대안**

| 대안 | 문제점 |
|------|--------|
| 메시지를 문자열 키로 저장 | ZSet의 `ZRANGEBYSCORE` 쿼리를 사용할 수 없어 재전송 구현이 복잡해짐 |
| 타임스탬프를 score로 사용 | 밀리초 단위 충돌 가능성, 동일 밀리초 내 순서 보장 불가 |
| **53비트 Snowflake 재설계** | 채택 — ZSet 호환성과 분산 고유성을 동시에 확보 |

**설계 결정**

53비트 범위 내에서 Snowflake를 재설계했습니다.

```
[ 41비트 타임스탬프 | 6비트 노드ID | 6비트 시퀀스 ] = 최대 2^53 - 1
  ~70년 지원        최대 63개 노드  노드당 63 msg/ms
```

```java
// double 변환 시 정밀도 손실 없음
return (now << TIMESTAMP_SHIFT) | (nodeId << NODE_SHIFT) | sequence;
```

**결과**: Redis ZSet과 100% 호환, 분산 환경 순서 보장, 노드당 63 msg/ms 처리량 확보

---

### 4. ID 비교 추상화 — 다양한 ID 포맷 지원

**문제 상황**

재전송 로직에서 메시지 ID를 숫자로 파싱해서 비교하고 있었습니다. 하지만 실제 운영 환경에서는 UUID, 날짜 기반 문자열 등 다양한 ID 포맷이 사용될 수 있습니다. ID 포맷에 따라 비교 로직이 달라지는데, 하드코딩되어 있으면 확장성이 제한됩니다.

**설계 결정**

ID 비교 로직을 `Comparator<String>` 인터페이스로 추상화했습니다.

```java
public class ReactiveSseAlertManager {
    private final Comparator<String> idComparator;

    private boolean isAfterRecovery(String eventId, AtomicReference<String> maxRecoveryIdRef) {
        if (eventId == null) return false;
        String maxId = maxRecoveryIdRef.get();
        if (maxId == null) return false;
        return idComparator.compare(eventId, maxId) > 0;
    }
}
```

기본 구현체로 `DefaultIdComparator`를 제공하며, 필요시 커스텀 Comparator를 주입할 수 있습니다.

```java
// 숫자 비교 (기본)
@Bean
Comparator<String> idComparator() {
    return Comparator.comparingLong(Long::parseLong);
}

// 또는 사전순
Comparator<String> idComparator() {
    return Comparator.naturalOrder();
}
```

**결과**: 다양한 ID 포맷 지원, 커스텀 비교 로직 주입 가능, 재전송 로직의 유연성 향상

---

### 5. 전송 경로 분리 — BasicSender vs FanoutSender

**문제 상황**

`AlertMessageDelegateSender`로 SSE와 Slack을 조합하면, Kafka Consumer에서 Redis Pub/Sub으로 메시지가 전파될 때 **모든 인스턴스가 Slack 웹훅을 호출**합니다. 3개의 인스턴스가 동작 중이라면 Slack 메시지가 3번 발송됩니다.

원인은 두 전송 수단의 **실행 경로가 근본적으로 다르다**는 데 있습니다:
- **SSE**: 클라이언트가 임의의 인스턴스에 연결되므로, 모든 인스턴스가 메시지를 받아 로컬 세션을 조회해야 합니다 → Redis Pub/Sub fan-out 필요
- **Slack**: 어느 인스턴스에서 실행해도 동일한 결과이므로, 한 번만 실행되어야 합니다 → Kafka Consumer 단계에서 직접 처리

**설계 결정**

`AlertMessageSender`를 두 서브인터페이스로 분리해 실행 경로를 타입으로 표현합니다.

```
AlertMessageSender (최상위)
  ├── FanoutAlertMessageSender   → 브로드캐스터 수신 Handler에서 호출 (모든 인스턴스)
  └── BasicAlertMessageSender    → 메시지 소비 Handler에서 직접 호출 (1회)
```

조합이 필요한 경우 타입별 DelegateSender를 제공합니다:

```java
// SSE + 커스텀 Fanout sender 조합
@Bean
FanoutAlertMessageSender fanoutSender(...) {
    return new FanoutAlertMessageDelegateSender(List.of(sseSender, customFanoutSender));
}

// Slack + Email 조합 — 메시지당 정확히 1회 실행 보장
@Bean
BasicAlertMessageSender basicSender(...) {
    return new BasicAlertMessageDelegateSender(List.of(slackSender, emailSender));
}
```

**결과**: 인스턴스 수와 무관하게 Slack 메시지 1회 발송, 타입만으로 실행 경로가 결정되어 설정 없이 자동 배선

---

### 6. DLQ 기반 실패 메시지 재처리 — 처리 실패 시 메시지 유실 방지

**문제 상황**

메시지 처리 중 예외가 발생하면 예외를 로그로만 기록하고 메시지가 유실됐습니다. 네트워크 장애나 일시적인 서비스 불가 상황에서도 재처리할 방법이 없었습니다.

**설계 결정**

Fixed Backoff 재시도와 DLQ(Dead Letter Queue) 전략을 조합했습니다. 처리에 실패한 메시지는 설정된 횟수만큼 일정 간격으로 재시도하고, 재시도를 소진하면 원본 토픽에 suffix를 붙인 DLQ 토픽으로 발행합니다. 실패 원인은 메시지 헤더(`x-orig-exception-message`)에 기록되어 장애 상황을 추적할 수 있습니다.

```
Kafka Consumer
  └── Fixed Backoff (interval, maxAttempts)
        ├── 재시도 중 → 동일 메시지 재처리
        └── 재시도 소진 → DLQ 토픽 발행 (원본 토픽 + suffix)
                            └── 실패 원인 헤더 첨부
```

```yaml
alert:
  kafka:
    dlq:
      topic-suffix: .dlq       # alert-topic → alert-topic.dlq (기본값)
      backoff:
        interval: 1000          # 재시도 간격 ms (기본값)
        max-attempts: 3         # 최대 재시도 횟수 (기본값)
```

**결과**: 재시도 소진 후 메시지 유실 대신 DLQ 격리, 실패 원인 헤더로 장애 추적 가능, DLQ 토픽을 별도 Consumer로 구독해 운영자가 재전송 여부를 판단할 수 있음

---

### 7. Last-Event-ID 기반 재전송 — SSE 재연결 시 누락 메시지 복구

**문제 상황**

SSE는 장시간 유지되는 HTTP 연결이기 때문에 네트워크 장애, 서버 재기동, 로드밸런서 타임아웃 등으로 연결이 끊어질 수 있습니다. 브라우저는 자동으로 재연결을 시도하지만, 단절 구간에 발행된 메시지는 유실됩니다.

**설계 결정**

메시지 전송 시 Cache에 메시지를 임시 저장하고, 재연결 시 SSE 스펙의 `Last-Event-ID` 헤더를 오프셋으로 활용해 누락 구간을 재전송합니다.

```
[단절 전]                    [단절 구간]               [재연결]
 msg#100 → 클라이언트 수신   msg#101 유실             Last-Event-ID: 100
                             msg#102 유실       → 서버: offset=100 이후 조회
                                                 msg#101, msg#102 재전송
```

Redis ZSet의 score로 Snowflake ID를 사용하므로, `ZRANGEBYSCORE key (lastEventId)`로 누락 구간을 정확하게 조회합니다. ZSet score가 double이라는 Redis 제약을 Snowflake 비트 설계 단계에서 이미 반영해 두었기 때문에 정밀도 손실 없이 동작합니다.

재연결 시 user 캐시, 각 tag 캐시, broadcast 캐시를 병합 조회한 뒤 ID 기준으로 중복을 제거합니다.

```java
fetchAndMerge(map, namespace, AlertTarget.id(subscriberId), offset);
for (String tag : tags) {
    fetchAndMerge(map, namespace, AlertTarget.tag(tag), offset);
}
fetchAndMerge(map, namespace, AlertTarget.broadcast(), offset);
```

**결과**: 재연결 시 누락 메시지 자동 복구, TTL로 메시지 보관 기한 자동 제한

---

### 8. Graceful Shutdown — SmartLifecycle Phase 제어 + Reactive Drain

**문제 발견**

Reactive Kafka/Redis 메시지 컨슈머를 구현하면서 애플리케이션 종료 시 두 가지 치명적인 위험을 발견했습니다:

- **리소스 누수**: `subscribe()`가 반환하는 `Disposable`을 명시적으로 관리하지 않아 종료 시 브로커와의 연결이 GC에 의존함
- **데이터 유실 (In-flight Message)**: 단순히 `dispose()`를 호출하면 처리 중이던 비즈니스 로직이 강제 중단되어 메시지가 유실됨

**해결 전략: 단계별 역순 종료 (Phase Control)**

메시지 흐름(`Broker → Cache → SSE`)의 역순으로 종료를 진행하기 위해 Spring의 `SmartLifecycle`을 도입하고 `phase` 값을 정밀하게 조정했습니다.

| 종료 순서 | Phase | 컴포넌트 | 이유 |
|-----------|-------|----------|------|
| 1순위 | MAX | Kafka Consumer | 가장 상위 소스 차단 |
| 2순위 | MAX - 1 | Redis Pub/Sub | 중간 전파 차단 |
| 3순위 | default | Spring Context | 기타 인프라 |

**핵심 메커니즘: Graceful Drain**

"새 메시지는 받지 않되, 이미 받은 메시지는 끝까지 처리한다"는 의미론을 구현하기 위해 세 가지 접근법을 비교했습니다:

| 접근법 | 방식 | 문제점 |
|--------|------|--------|
| `dispose()` 즉시 호출 | 스트림 강제 취소 | in-flight 메시지 중단 |
| `timeout(Duration)` 대기 | 일정 시간 대기 후 취소 | 처리 완료 시점을 알 수 없어 과도하게 대기하거나 조기 종료 |
| **`takeUntilOther` + `Mono.cache()` + `block()`** | 완료 신호 전파 후 drain 대기 | 채택 |

`takeUntilOther(stopSink.asMono())`는 upstream의 구독을 해제하지만, `flatMap` 내부에서 이미 시작된 inner publisher는 완료까지 실행을 유지합니다. "새 메시지는 받지 않되, 처리 중인 메시지는 완료한다"는 의미론을 정확히 표현합니다.

```java
// ReactiveKafkaMessageConsumerRegistrar
private void startConsumer(String topic, ReactiveTopicAlertMessageHandler<R> handler, Integer concurrency) {
    Sinks.Empty<Void> stopSink = Sinks.empty();
    topicStopSinks.put(topic, stopSink);

    Mono<Void> completion = KafkaReceiver.create(receiverOptions)
            .receive()
            .takeUntilOther(stopSink.asMono())  // 종료 신호 수신 시 새 메시지 수신 중단
            .flatMap(record -> processMessage(record, handler, dlqTopic), concurrency)
            .then()
            .cache();  // subscribe + block 모두 동일한 upstream 참조

    topicCompletionMonos.put(topic, completion);
    completion.subscribe();
}

@Override
public synchronized void stop() {
    running = false;
    topicStopSinks.values().forEach(Sinks.Empty::tryEmitEmpty);  // 종료 신호

    try {
        Mono.when(topicCompletionMonos.values())
                .block(Duration.ofSeconds(30));  // in-flight 메시지 처리 완료 대기
    } catch (Exception e) {
        log.warn("Timeout or error while draining Kafka consumers: {}", e.getMessage());
    }

    topicStopSinks.clear();
    topicCompletionMonos.clear();
}

@Override
public int getPhase() {
    return Integer.MAX_VALUE;  // 가장 먼저 종료
}
```

**`Mono.cache()`를 사용하는 이유**: `subscribe()`와 `stop()`의 `block()` 두 곳이 동일한 upstream을 바라봐야 합니다. `cache()` 없이는 `block()`이 새 upstream을 생성해 이미 완료된 스트림이 다시 시작됩니다.

**재등록 시 종료 신호 처리**: 런타임에 `register()`가 다시 호출되면 기존 리스너에 종료 신호를 전송하고 새 리스너를 시작합니다. 기존 `stopSink`를 제거하고 `tryEmitEmpty()`로 종료 이벤트를 보내, 기존 스트림이 graceful drain을 거치도록 합니다.

**결과**: 메시지 브로커 → 캐시/Pub-Sub → Spring Context 순서 보장, 처리 중인 메시지 보호, 런타임 재등록 지원

---

### 9. PartitionKeyStrategy — 순서 보장 전략 주입

`KafkaMessagePublisher`에서 `PartitionKeyStrategy`를 사용해 메시지를 대기열별로 하나의 파티션으로 전송할 수 있습니다.

namespace 또는 특정 속성별로 강력한 순서 보장을 원한다면 `PartitionKeyStrategy`를 구현할 수 있습니다.

```java
@FunctionalInterface
public interface PartitionKeyStrategy {
    String resolve(AlertMessage message);

    static PartitionKeyStrategy none() { return msg -> null; }                     // 기본 파티셔너
    static PartitionKeyStrategy byNamespace() { return AlertMessage::namespace; }  // namespace별 순서 보장
}
```

---

## 확장 포인트

각 레이어가 인터페이스로 추상화되어 있어, Bean 교체만으로 브로커/전송 수단/저장소를 변경할 수 있습니다.

| 인터페이스 | 역할 | 기본 구현체 | 교체 예시 |
|---|---|---|---|
| `AlertMessagePublisher` | 메시지 발행 | `KafkaMessagePublisher` | RabbitMQ, SQS |
| `MessageConsumerRegistrar` | 메시지 소비 | `KafkaMessageConsumerRegistrar` | RabbitMQ, SQS |
| `MessageBroadcaster` | 인스턴스 전파 (송신) | `RedisMessageBroadcaster` | DB polling, ZooKeeper |
| `AlertMessageListenerRegistrar` | 인스턴스 전파 (수신) | `RedisMessageListenerRegistrar` | DB polling, webhook |
| `FanoutAlertMessageSender` | 팬아웃 전송 | `SseAlertMessageSender` | WebSocket |
| `BasicAlertMessageSender` | 단일 전송 | — | Slack, Email |
| `AlertCacheManager` | 메시지 캐시 | `RedisAlertCacheManager` | 커스텀 캐시 |
| `IdGenerator` | ID 생성 | `SnowflakeIdGenerator` | UUID |
| `CommonErrorHandler` | 소비 실패 처리 | `DefaultErrorHandler` (DLQ) | 지수 backoff |

---

## Virtual Threads 지원 (Java 21+)

Java 21의 Virtual Threads를 활용하여 블로킹 작업의 스레드 효율을 높일 수 있습니다.

**중요**: Blocking 모드(`alert.reactive=false`, 기본값)에서만 적용됩니다. Reactive 모드에서는 Reactor의 `boundedElastic` 정책을 따릅니다.

### 활성화 방법

Spring Boot 3.2+에서 아래 설정만 추가하면 됩니다:

```yaml
spring:
  threads:
    virtual:
      enabled: true
```

Spring Boot가 virtual thread 기반 `AsyncTaskExecutor`를 자동 생성하고, 라이브러리가 `ObjectProvider<AsyncTaskExecutor>`로 감지하여 적용합니다. `AsyncTaskExecutor` 빈이 없으면 동기 실행(현재 스레드)으로 동작합니다.

### 적용 범위

`AsyncTaskExecutor` 빈이 컨텍스트에 존재하면 다음 경로에 적용됩니다:

| 컴포넌트 | 적용 내용 |
|---------|----------|
| **SSE (MVC)** | `SseAlertManager` — 구독/발행/재전송을 비동기 디스패치 |
| **Kafka Consumer** | `ConcurrentKafkaListenerContainerFactory` — 리스너 태스크 실행자 |
| **Redis Pub/Sub** | `RedisMessageListenerContainer` — 태스크 및 구독 실행자 |

### 커스텀 Executor 사용

별도의 executor 정책(격리/큐잉/제한)이 필요하면 전용 `AsyncTaskExecutor` 빈을 등록하고, `AlertManager`를 직접 선언하여 `@Qualifier`로 주입합니다.

```java
@Bean("myAlertExecutor")
AsyncTaskExecutor myAlertExecutor() {
    SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("my-alert-");
    executor.setVirtualThreads(true);
    return executor;
}

@Bean
AlertManager customAlertManager(
        TagBasedAlertSessionRepository<SseEmitter> emitterRepository,
        AlertCacheManager alertCacheManager,
        AlertMessageFactory alertMessageFactory,
        AlertMessagePublisher alertMessagePublisher,
        AlertMessageSupport alertMessageSupport,
        @Qualifier("myAlertExecutor") AsyncTaskExecutor executor
) {
    return new SseAlertManager(
            alertMessagePublisher, alertMessageFactory, emitterRepository,
            alertCacheManager, alertMessageSupport, DefaultAlertMessage.class, executor
    );
}
```

### 주의사항

- `AsyncTaskExecutor` 빈이 없으면 기존과 동일하게 동기 실행됩니다. 기존 동작에 영향을 주지 않습니다.
- Kafka의 경우 Virtual Threads는 **poll/listener 스레드**에 적용되며, per-record 병렬 처리는 아닙니다.
- Java 21 이상이 필요합니다.

---

## Auto-Configuration 설계

라이브러리 사용자의 classpath에 따라 필요한 설정만 활성화됩니다.

| 설정 클래스 | 활성화 조건 |
|---|---|
| `AlertConfig` | 항상 (core) |
| `AlertKafkaConfig` | `KafkaTemplate` on classpath + blocking 모드 |
| `AlertReactiveKafkaConfig` | `KafkaSender` on classpath + reactive 모드 |
| `AlertRedisConfig` | `RedisConnectionFactory` on classpath |

- `@ConditionalOnClass`로 없는 의존성에 대한 `ClassNotFoundException` 방지
- Kafka, Redis, Slack은 `compileOnly` 스코프로 사용자에게 전이되지 않음
- Spring Boot BOM(`platform`)을 사용하여 버전 관리를 사용자 측에 위임

---

## 테스트

213개 테스트 케이스, JaCoCo 92% 커버리지.

격리된 테스트 환경 구성을 위해 Testcontainers와 Embedded Redis를 활용합니다.

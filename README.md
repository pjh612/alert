# # 목차

1. [# 소개](#-소개)
2. [# Dependency 다이어그램](#-dependency-다이어그램)
3. [# 활용](#-활용)
    - [AlertMessageDelegateSender로 여러 채널로 메시지 보내기](#alertmessagedelegatesender로-여러-채널로-메시지-보내기)
    - [MessagePublisher를 Kafka가 아닌 RabbitMQ로 바꿔보기](#messagepublisher를-kafka가-아닌-rabbitmq로-바꿔보기)
    - [MessageBroadcaster를 Redis pub/sub이 아닌 Kafka로 바꿔보기](#messagebroadcaster를-redis-pubsub이-아닌-kafka로-바꿔보기)
4. [# 구성 요소 상세](#-구성-요소-상세)
    - [AlertManager](#alertmanager)
        - [SubscribableAlertManager](#subscribablealertmanager)
        - [AbstractAlertManager](#abstractalertmanager)
    - [AlertMessageFactory](#alertmessagefactory)
    - [SseAlertManager](#ssealertmanager)
    - [AlertMessagePublisher](#alertmessagepublisher)
    - [MessageConsumerRegistrar](#messageconsumerregistrar)
    - [TopicAlertMessageHandler](#topicalertmessagehandler)
    - [MessageBroadcaster](#messagebroadcaster)
    - [AlertMessageSender](#alertmessagesender)
        - [SseAlertMessageSender](#ssealertmessagesender)
        - [SlackAlertMessageSender](#slackalertmessagesender)
        - [AlertMessageDelegateSender](#alertmessagedelegatesender)
5. [# AlertChannel, AlertMessage, AlertMessageType](#alertchannel-alertmessage-alertmessagetype)
    - [AlertChannel](#alertchannel)
    - [AlertMessage](#alertmessage)
    - [AlertMessageType](#alertmessagetype)


# # 소개
이 모듈은 알림 기능을 손쉽게 적용할 수 있도록 구성된 재사용 가능한 모듈입니다.

메시지는 publish되면 consumer에 의해 소비되고, 소비된 메시지는 MessageBroadcaster를 통해 구독 중인 모든 노드로 전파되며, 최종적으로 AlertMessageSender에 의해 다양한 채널로 전송됩니다.

이 과정은 발행 → 소비 → 브로드캐스트 → 전송의 4단계로, 각 컴포넌트는 인터페이스 기반으로 추상화되어 있어 브로커 종류(Kafka, RabbitMQ 등), 브로드캐스트 방식(Redis pub/sub 등), 전송 채널(Slack, Email, SSE 등)에 따라 쉽게 교체하거나 확장할 수 있습니다.

기본 구성은 Kafka + Redis를 사용하지만,
MessagePublisher를 교체하면 다른 메시지 브로커를 사용할 수 있고

MessageBroadcaster를 교체하면 브로드캐스팅 방식도 변경할 수 있습니다.

AlertMessageSender 구현체를 추가하면 Slack, Teams, Email, SMS, SSE 등 다양한 방식으로 메시지를 전송할 수 있습니다.

# # Dependency 다이어그램
![RedisMessageBroadcaster](https://github.com/user-attachments/assets/8c2a2224-9a6e-438b-a3f8-f08b2fab7c29)


# # 활용
## AlertMessageDelegateSender로 여러 채널로 메시지 보내기
AlertMessageDelegateSender를 사용해 한 메시지를 여러 채널로 전송할 수 있습니다.
SSE를 통해 클라이언트로 메시지를 보내고, Slack을 통해 알림 채널로 메시지를 보내는 예제입니다.

**Bean 등록**
MessageListener는 AlertMessageSender 인터페이스만을 의존하기 때문에 AlertMessageSender를 구현하여 빈으로 등록하면 교체해줄 수 있습니다.

AlertMessageDelegateSender는 AlertMessageSender를 리스트 형태로 가지기 때문에 한번에 여러 Sender 구현체를 사용할 수 있습니다.

```java
@Configuration
public class AlertConfig {


   @Bean
   AlertMessageSender alertMessageSender(EmitterRepository emitterRepository, @Value("${alert.slack.webhook.url}") String webhookUrl, ObjectMapper objectMapper) {
       return new AlertMessageDelegateSender(List.of(new SseAlertMessageSender(emitterRepository), new SlackAlertMessageSender(webhookUrl, messageConverter(objectMapper), "#결제알림")));
   }


   MessageConverter<AlertMessage, String> messageConverter(ObjectMapper objectMapper) {
       return new ConfirmPaymentAlertFormatter(objectMapper);
   }
}
```


**MessageConverter**
MessageConverter를 다음과 같이 구현하여 메시지를 AlertMessage 객체 형태로 전달받은 메시지를 Slack과 같은 메신저에서 사용자 친화적으로 읽을 수 있도록 변환 해줄 수 있습니다.

```java
@Override
public String convert(AlertMessage message) {
   PaymentConfirmResponse confirm = objectMapper.convertValue(message.body(), PaymentConfirmResponse.class);


   return String.format("""
       ✅ [결제 승인 완료]


       • 요청 ID: %s
       • 주문 ID: %s
       • 회원 ID: %s
       • 요청 금액: %,.0f원
       • 클라이언트 ID: %s
       • 결제 상태: %d
       • 실패 사유: %s
       • 승인 시각: %s
       """,
           confirm.requestId(),
           confirm.orderId(),
           confirm.requestMemberId(),
           confirm.requestPrice(),
           confirm.clientId(),
           confirm.paymentStatus(),
           confirm.failureReason() == null ? "(없음)" : confirm.failureReason(),
           confirm.approvedAt()
   );
}
```

## MessagePublisher를 Kafka가 아닌 RabbitMQ로 바꿔보기
MessagePublisher를 Kafka 대신 RabbitMQ를 사용하고 싶을 때에는 MessagePublisher Bean을 RabbitMQ 구현체로 변경해주면 됩니다.

**RabbitMQ Publisher 구현**
RabbitMQ를 사용해서 메시지를 publish할 MessagePublisher를 구현합니다.
```java
public class RabbitMqMessagePublisher implements AlertMessagePublisher {
   private final RabbitTemplate rabbitTemplate;


   private static final String DEFAULT_ROUTING_KEY = "default";


   public RabbitMqMessagePublisher(RabbitTemplate rabbitTemplate) {
       this.rabbitTemplate = rabbitTemplate;
   }


   @Override
   public void publish(String channel, AlertMessage message) {
       rabbitTemplate.convertAndSend(channel, DEFAULT_ROUTING_KEY, message);
   }
}
```

**Message Listener 구현 및 등록**
발행한 메시지를 수신할 Listener를 등록합니다.
```java
@RabbitListener(queues = "PAYMENT_RESULT")
public void onMessage(AlertMessage message) throws JsonProcessingException {
   messageBroadcaster.sendMessage("PAYMENT_RESULT", objectMapper.writeValueAsString(message));
}
```

**빈 등록**
구현한 RabbitMQ Publisher를 빈으로 등록해줍니다.
```java
@Bean
public AlertMessagePublisher alertMessagePublisher(RabbitTemplate rabbitTemplate) {
   return new RabbitMqMessagePublisher(rabbitTemplate);
}
```

## MessageBroadcaster를 Redis pub/sub이 아닌 Kafka로 바꿔보기
기존 브로드 캐스팅 방식을 MessageBroadcaster를 구현하여 기본 설정인 Redis Pub/Sub에서 Kafka 사용으로 변경할 수 있습니다.

**KafkaMessageBroadcaster 구현**
KafkaTemplate을 사용하여 MessageBroadCaster를 구현해줍니다.

```java
public class KafkaMessageBroadcaster implements MessageBroadcaster<String> {
   private final KafkaTemplate<String, String> kafkaTemplate;


   public KafkaMessageBroadcaster(KafkaTemplate<String, String> kafkaTemplate) {
       this.kafkaTemplate = kafkaTemplate;
   }


   @Override
   public void sendMessage(String topic, String message) {
       kafkaTemplate.send(topic, message);
   }
}
```
**빈 등록**
```java
@Bean
public MessageBroadcaster<String> messageBroadcaster(KafkaTemplate<String, String> kafkaTemplate) {
   return new KafkaMessageBroadcaster(kafkaTemplate);
}
```

```java
@Bean
public ConsumerFactory<String, AlertMessage> alertConsumerFactory(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers, @Value("${groupId}") String groupId) {
   Map<String, Object> props = new HashMap<>();
   props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
   props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
   props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
   props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
   props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
   return new DefaultKafkaConsumerFactory<>(props);
}
```

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, AlertMessage> alertKafkaListenerContainerFactory(ConsumerFactory<String, AlertMessage> alertConsumerFactory) {
   ConcurrentKafkaListenerContainerFactory<String, AlertMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
   factory.setConsumerFactory(alertConsumerFactory);
   factory.setRecordMessageConverter(new StringJsonMessageConverter());
   factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
   return factory;
}
```

```java
@Component
public class KafkaConsumer {
   private final AlertMessageHandler alertMessageHandler;
   public KafkaConsumer(AlertMessageHandler alertMessageHandler) {
       this.alertMessageHandler = alertMessageHandler;
   }


   @KafkaListener(
           topics = "${topic.paymentConfirm}",
           groupId = "${groupId}",
           containerFactory = "alertKafkaListenerContainerFactory"
   )
   public void handle(


           @Payload DefaultAlertMessage payload) {
       alertMessageHandler.handle(payload);
   }
}
```

> 🚨Group ID
>
> 분산 환경에서 SSE 알림 전송을 위해서 클라이언트와 연결된 유효한 Emitter를 찾아야합니다.
그렇게 하기 위해서는 Kafka는 하나의 메시지를 하나의 Group에 속한 컨슈머 간에는 메시지를 중복 소비하지 않기 때문에 메시지를 각 서버 노드에 위치한 Consumer가 모두 처리하기 위해서는 Group ID를 서버 노드마다 다르게 하여 독립적으로 처리할 수 있도록 합니다.



# # 구성 요소 상세
## AlertManager
사용자의 알림 발송 진입점입니다.
```java
AlertManager
public interface AlertManager {
    void notice(AlertChannel alertChannel, String targetId, Object message);
}
```

메시지를 발송 인터페이스를 정의합니다.
단순 전송을 담당하는 AbstractAlertManager가 있고, SSE와 같이 구독이 필요하다면 SubscribableAlertManager를 구현하여 SseAlertManager와 같이 사용할 수 있습니다.
### SubscribableAlertManager
AlertManager를 상속받고 구독가능한 형태의 구현을 위한 인터페이스입니다.
```java
public interface SubscribableAlertManager<T> extends AlertManager {
    T subscribe(AlertChannel alertChannel, String subscriberId, String lastEventId, Long timeoutMillis);
}
```



### AbstractAlertManager
메시지 payload를 받아 AlertMessageFactory를 통해 메시지를 생성하고 MessagePublisher를 통해 메시지를 발행합니다.


## AlertMessageFactory
메시지 payload를 전달받아 AlertManager가 전송할 수 있는 형태의 메시지를 생성합니다.
```java
public interface AlertMessageFactory {
    AlertMessage onConnect(String targetId);


    AlertMessage onReplayMessage(String targetId, Object message);


    AlertMessage onMessage(String targetId, Object message);
}
```

## SseAlertManager
메시지를 SSE를 기반으로 전달하는 AlertManager의 구현체 입니다.
구독 시 SseEmitter 객체 생성하고 EmitterRepository에 저장하며 클라이언트로 전달이 누락된 메시지는 캐시 매니저를 통해 가져와 재전송 해줄 수 있습니다.

## AlertMessagePublisher
메시지를 publish하는 역할로, Kafka, RabbitMQ 등을 사용하여 구현 가능합니다.
발행된 메시지는 MessageConsumerRegistrar을 통해 등록된 Consumer가 TopicAlertMessageHandler를 실행시키며 메시지가 처리 됩니다.

## MessageConsumerRegistrar
메시지를 소비할 컨슈머를 빈으로 등록합니다.
```java
public interface MessageConsumerRegistrar {
    void register(String topic, TopicAlertMessageHandler messageHandler);
}
```


## TopicAlertMessageHandler
소비 시 어떤 동작을 할지는 TopicAlertMessageHandler가 정의합니다
```java
@FunctionalInterface
public interface TopicAlertMessageHandler {
    void handle(String topic, AlertMessage message);
}
```

## MessageBroadcaster
MessagePublisher에 의해 발행된 메시지가 소비되며 MessageBroadcaster를 사용할 수 있습니다.
예를들어, RedisMessageBroadcaster인 경우 Redis에 메시지를 publish하고 등록된 MessageListener에 의해 AlertMessageSender를 통해 최종 전송할 수 있습니다.
```java
@FunctionalInterface
public interface MessageBroadcaster<T> {
    void sendMessage(String topic, T message);
}
```


## AlertMessageSender
AlertMessageSender는 다양하게 구현할 수 있습니다. 알림 모듈은 기본적으로 3가지의 구현체를 제공합니다.
### SseAlertMessageSender
AlertMessage를 SseEmitter로 변환하여 클라이언트로 전달됩니다.
### SlackAlertMessageSender
slack 채널을 통해 메시지를 전달합니다.
MessageFormatter를 구현하여 AlertMessage를 원하는 형태의 메시지로 포맷팅하여 전송할 수 있습니다.
### AlertMessageDelegateSender
AlertMessageSender 리스트를 사용하여 각각의 sender를 통해 메시지를 전달합니다.
예시로, SSE전송과 Slack 알림을 함께 전송하고 싶을때 사용할 수 있습니다.
## AlertChannel, AlertMessage, AlertMessageType,
### AlertChannel
전송할 메시지가 전송될 채널을 정의합니다.
Kafka라면 topic에 매핑될 수 있고, RabbitMQ 라면 큐 이름이 될 수 있습니다. 또는 MessagePublisher 구현체에서 자유롭게 활용 가능합니다.
```java
public interface AlertChannel {
    String name();
}
```


**구현 예시**
```java
public enum DefaultAlertChannel implements AlertChannel {
    PAYMENT_RESULT
}
```


### AlertMessage
알림 모듈 내에서 사용되는 메시지 인터페이스로 메시지가 전달될 대상의 식별자, 메시지의 유형, 메시지 내용, 메시지 재전송 여부를 정의합니다.
public interface AlertMessage {
AlertMessageType type();

    String targetId();

    Object body();

    boolean isReplay();
}


**구현 예시**
```java
public record DefaultAlertMessage(String targetId, DefaultAlertMessageType type, Object body, boolean isReplay) implements AlertMessage {
}
```



### AlertMessageType
메시지의 유형을 정의하고 AlertCacheManager가 메시지를 캐싱할지를 판단할 수 있는 인터페이스입니다.
```java
public interface AlertMessageType {
    String type();

    boolean isCacheable();

    String toString();
}
```

**구현 예시**
```java
public enum DefaultAlertMessageType implements AlertMessageType {
    CONNECT("connect"),
    MESSAGE("message");

    private final String type;

    DefaultAlertMessageType(String type) {
        this.type = type;
    }

    @Override
    public String type() {
        return this.type;
    }

    @Override
    public boolean isCacheable() {
        return this == MESSAGE;
    }
}
```



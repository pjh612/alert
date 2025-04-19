package com.alert.core.messaging.bridge;


import com.alert.core.messaging.model.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.Map;

public class ReactiveKafkaMessageConsumerRegistrar implements MessageConsumerRegistrar {
    private final Map<String, Object> properties;

    private static final Logger log = LoggerFactory.getLogger(ReactiveKafkaMessageConsumerRegistrar.class);

    public ReactiveKafkaMessageConsumerRegistrar(Map<String, Object> properties) {
        this.properties = properties;
    }

    private ReceiverOptions<String, AlertMessage> createReceiverOptions(String topic) {
        return ReceiverOptions.<String, AlertMessage>create(properties)
                .subscription(Collections.singleton(topic));
    }

    public void register(String topic, TopicAlertMessageHandler messageHandler) {
        ReceiverOptions<String, AlertMessage> receiverOptions = createReceiverOptions(topic);

        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(record -> {
                    if (log.isTraceEnabled()) {
                        log.trace("Alert Message Consumer consumes alert message: {}", record.value());
                    }
                    messageHandler.handle(record.topic(), record.value());
                    record.receiverOffset().acknowledge(); // 수동 Offset 처리
                })
                .subscribe();
    }
}


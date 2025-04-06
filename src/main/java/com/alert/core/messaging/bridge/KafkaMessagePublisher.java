package com.alert.core.messaging.bridge;

import com.alert.core.messaging.model.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaMessagePublisher<T extends AlertMessage> implements MessagePublisher<T> {
    private final KafkaTemplate<String, T> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(KafkaMessagePublisher.class);

    public KafkaMessagePublisher(KafkaTemplate<String, T> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(String channel, T message) {
        log.debug("channel:{}, alertMessage:{}", channel, message);

        kafkaTemplate.send(channel, message);
    }
}

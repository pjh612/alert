package com.alert.core.messaging.bridge;

import com.alert.core.messaging.model.AlertMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

public class ReactiveKafkaAlertMessagePublisher implements ReactiveAlertMessagePublisher {
    private static final Logger log = LoggerFactory.getLogger(ReactiveKafkaAlertMessagePublisher.class);

    private final KafkaSender<String, AlertMessage> kafkaSender;

    public ReactiveKafkaAlertMessagePublisher(KafkaSender<String, AlertMessage> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @Override
    public Mono<Void> publish(String channel, AlertMessage message) {
        log.debug("channel:{}, alertMessage:{}", channel, message);

        ProducerRecord<String, AlertMessage> producerRecord = new ProducerRecord<>(channel, message);

        SenderRecord<String, AlertMessage, String> senderRecord = SenderRecord.create(producerRecord, null);

        return kafkaSender.send(Mono.just(senderRecord))
                .doOnTerminate(() -> log.debug("Message sent to Kafka, channel: {}, alertMessage: {}", channel, message))
                .then();
    }
}

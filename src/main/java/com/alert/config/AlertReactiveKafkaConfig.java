package com.alert.config;

import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.broadcaster.ReactiveMessageBroadcaster;
import com.alert.core.messaging.consumer.ReactiveAlertMessageBroadcastHandler;
import com.alert.core.messaging.consumer.ReactiveMessageConsumerRegistrar;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.publisher.ReactiveAlertMessagePublisher;
import com.alert.core.messaging.sender.BasicAlertMessageDelegateSender;
import com.alert.core.messaging.sender.BasicAlertMessageSender;
import com.alert.infra.kafka.PartitionKeyStrategy;
import com.alert.infra.kafka.ReactiveKafkaAlertMessagePublisher;
import com.alert.infra.kafka.ReactiveKafkaMessageConsumerRegistrar;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@ConditionalOnClass(KafkaSender.class)
@EnableConfigurationProperties(AlertKafkaProperties.class)
@ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
public class AlertReactiveKafkaConfig {

    private final AlertProperties alertProperties;
    private final AlertKafkaProperties alertKafkaProperties;

    public AlertReactiveKafkaConfig(AlertProperties alertProperties, AlertKafkaProperties alertKafkaProperties) {
        this.alertProperties = alertProperties;
        this.alertKafkaProperties = alertKafkaProperties;
    }

    @Bean
    public KafkaSender<String, AlertMessage> kafkaSender(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 5);
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        SenderOptions<String, AlertMessage> senderOptions = SenderOptions.create(producerProps);
        return KafkaSender.create(senderOptions);
    }

    @Bean
    @ConditionalOnMissingBean(PartitionKeyStrategy.class)
    public PartitionKeyStrategy partitionKeyStrategy() {
        return PartitionKeyStrategy.none();
    }

    @Bean
    ReactiveAlertMessagePublisher reactiveAlertMessagePublisher(KafkaSender<String, AlertMessage> kafkaSender, AlertMessageSupport alertMessageSupport, PartitionKeyStrategy partitionKeyStrategy, ReactiveAlertCacheManager reactiveAlertCacheManager) {
        return new ReactiveKafkaAlertMessagePublisher(reactiveAlertCacheManager, alertMessageSupport, kafkaSender, partitionKeyStrategy);
    }

    @Bean
    @ConditionalOnMissingBean(ReactiveMessageConsumerRegistrar.class)
    public <T> ReactiveMessageConsumerRegistrar<T> reactiveMessageConsumerRegistrar(ReactiveMessageBroadcaster<String, T> messageBroadcaster, ObjectMapper objectMapper, @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                                                                    ObjectProvider<BasicAlertMessageSender> basicSenderProvider,
                                                                                    KafkaSender<String, AlertMessage> kafkaSender) {
        BasicAlertMessageSender basicSender = resolveBasicSender(basicSenderProvider);
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, alertProperties.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class.getName());
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        ReactiveKafkaMessageConsumerRegistrar<T> kafkaMessageConsumerRegistrar = new ReactiveKafkaMessageConsumerRegistrar<>(props, alertKafkaProperties.dlq(), kafkaSender);

        List<String> topics = alertProperties.topics();
        for (String topic : topics) {
            kafkaMessageConsumerRegistrar.register(topic, new ReactiveAlertMessageBroadcastHandler<>(messageBroadcaster, objectMapper, basicSender), alertProperties.concurrency());
        }

        return kafkaMessageConsumerRegistrar;
    }

    private BasicAlertMessageSender resolveBasicSender(ObjectProvider<BasicAlertMessageSender> provider) {
        List<BasicAlertMessageSender> senders = provider.stream().toList();
        if (senders.isEmpty()) return null;
        if (senders.size() == 1) return senders.get(0);
        return new BasicAlertMessageDelegateSender(senders);
    }
}

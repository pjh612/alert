package com.alert.config;

import com.alert.core.messaging.bridge.AlertMessageBroadcastHandler;
import com.alert.core.messaging.bridge.AlertMessagePublisher;
import com.alert.core.messaging.bridge.KafkaMessageConsumerRegistrar;
import com.alert.core.messaging.bridge.KafkaMessagePublisher;
import com.alert.core.messaging.bridge.MessageConsumerRegistrar;
import com.alert.core.messaging.bridge.ReactiveAlertMessageBroadcastHandler;
import com.alert.core.messaging.bridge.ReactiveAlertMessagePublisher;
import com.alert.core.messaging.bridge.ReactiveKafkaAlertMessagePublisher;
import com.alert.core.messaging.bridge.ReactiveKafkaMessageConsumerRegistrar;
import com.alert.core.messaging.bridge.ReactiveMessageConsumerRegistrar;
import com.alert.core.messaging.broadcaster.MessageBroadcaster;
import com.alert.core.messaging.broadcaster.ReactiveMessageBroadcaster;
import com.alert.core.messaging.model.AlertMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@ConditionalOnProperty(
        name = "alert.bridge",
        havingValue = "kafka",
        matchIfMissing = true
)
public class AlertKafkaConfig {

    private final AlertProperties alertProperties;

    public AlertKafkaConfig(AlertProperties alertProperties) {
        this.alertProperties = alertProperties;
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    public ProducerFactory<String, AlertMessage> producerFactory(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean("alertKafkaTemplate")
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    public KafkaTemplate<String, AlertMessage> alertKafkaTemplate(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaTemplate<>(producerFactory(bootstrapServers));
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    public KafkaSender<String, AlertMessage> kafkaSender(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        SenderOptions<String, AlertMessage> senderOptions = SenderOptions.create(producerProps);
        return KafkaSender.create(senderOptions);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    @ConditionalOnMissingBean(AlertMessagePublisher.class)
    public AlertMessagePublisher alertMessagePublisher(KafkaTemplate<String, AlertMessage> alertKafkaTemplate) {
        return new KafkaMessagePublisher(alertKafkaTemplate);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    ReactiveAlertMessagePublisher reactiveAlertMessagePublisher(KafkaSender<String, AlertMessage> kafkaSender) {
        return new ReactiveKafkaAlertMessagePublisher(kafkaSender);
    }

    @Bean("alertConsumerFactory")
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    public ConsumerFactory<String, AlertMessage> alertConsumerFactory(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, alertProperties.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean("alertKafkaListenerContainerFactory")
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    public ConcurrentKafkaListenerContainerFactory<String, AlertMessage> alertKafkaListenerContainerFactory(@Qualifier("alertConsumerFactory") ConsumerFactory<String, AlertMessage> alertConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, AlertMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(alertConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean(MessageConsumerRegistrar.class)
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    public MessageConsumerRegistrar messageConsumerRegistrar(ConcurrentKafkaListenerContainerFactory<String, AlertMessage> alertKafkaListenerContainerFactory, ApplicationContext applicationContext,
                                                             MessageBroadcaster<String> messageBroadcaster, ObjectMapper objectMapper) {
        KafkaMessageConsumerRegistrar kafkaMessageConsumerRegistrar = new KafkaMessageConsumerRegistrar(alertKafkaListenerContainerFactory, applicationContext);
        List<String> topics = alertProperties.topics();
        for (String topic : topics) {
            kafkaMessageConsumerRegistrar.register(topic, new AlertMessageBroadcastHandler(messageBroadcaster, objectMapper));
        }

        return kafkaMessageConsumerRegistrar;
    }

    @Bean
    @ConditionalOnMissingBean(MessageConsumerRegistrar.class)
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    public <T> ReactiveMessageConsumerRegistrar<T> reactiveMessageConsumerRegistrar(ReactiveMessageBroadcaster<String, T> messageBroadcaster, ObjectMapper objectMapper, @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, alertProperties.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");
        ReactiveKafkaMessageConsumerRegistrar<T> kafkaMessageConsumerRegistrar = new ReactiveKafkaMessageConsumerRegistrar<>(props);

        List<String> topics = alertProperties.topics();
        for (String topic : topics) {
            kafkaMessageConsumerRegistrar.register(topic, new ReactiveAlertMessageBroadcastHandler<>(messageBroadcaster, objectMapper), alertProperties.concurrency());
        }

        return kafkaMessageConsumerRegistrar;
    }
}

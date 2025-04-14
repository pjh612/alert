package com.alert.config;

import com.alert.core.messaging.bridge.*;
import com.alert.core.messaging.broadcaster.MessageBroadcaster;
import com.alert.core.messaging.model.AlertMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

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
    public ProducerFactory<String, AlertMessage> producerFactory(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean("alertKafkaTemplate")
    public KafkaTemplate<String, AlertMessage> alertKafkaTemplate(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaTemplate<>(producerFactory(bootstrapServers));
    }

    @Bean
    @ConditionalOnMissingBean(AlertMessagePublisher.class)
    public AlertMessagePublisher alertMessagePublisher(KafkaTemplate<String, AlertMessage> alertKafkaTemplate) {
        return new KafkaMessagePublisher(alertKafkaTemplate);
    }

    @Bean("alertConsumerFactory")
    public ConsumerFactory<String, AlertMessage> alertConsumerFactory(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, alertProperties.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean("alertKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, AlertMessage> alertKafkaListenerContainerFactory(@Qualifier("alertConsumerFactory") ConsumerFactory<String, AlertMessage> alertConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, AlertMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(alertConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean(MessageConsumerRegistrar.class)
    public MessageConsumerRegistrar messageConsumerRegistrar(ConcurrentKafkaListenerContainerFactory<String, AlertMessage> alertKafkaListenerContainerFactory, ApplicationContext applicationContext,
                                                             MessageBroadcaster<String> messageBroadcaster, ObjectMapper objectMapper) {
        KafkaMessageConsumerRegistrar kafkaMessageConsumerRegistrar = new KafkaMessageConsumerRegistrar(alertKafkaListenerContainerFactory, applicationContext);
        List<String> topics = alertProperties.topics();
        for (String topic : topics) {
            kafkaMessageConsumerRegistrar.register(topic, new AlertMessageBroadcastHandler(messageBroadcaster, objectMapper));
        }

        return kafkaMessageConsumerRegistrar;
    }
}

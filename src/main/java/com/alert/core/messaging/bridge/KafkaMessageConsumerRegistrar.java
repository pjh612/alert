package com.alert.core.messaging.bridge;

import com.alert.core.messaging.model.AlertMessage;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

public class KafkaMessageConsumerRegistrar<T extends AlertMessage> implements MessageConsumerRegistrar {
    private final ConcurrentKafkaListenerContainerFactory<String, T> kafkaListenerContainerFactory;
    private final GenericApplicationContext context;

    public KafkaMessageConsumerRegistrar(ConcurrentKafkaListenerContainerFactory<String, T> kafkaListenerContainerFactory, ApplicationContext context) {
        this.kafkaListenerContainerFactory = kafkaListenerContainerFactory;
        this.context = (GenericApplicationContext) context;
    }

    @Override
    public void register(String topic, TopicAlertMessageHandler messageHandler) {
        ConcurrentMessageListenerContainer<String, T> container = kafkaListenerContainerFactory.createContainer(topic);
        ContainerProperties containerProperties = container.getContainerProperties();
        containerProperties.setMessageListener((MessageListener<String, T>) record ->
                messageHandler.handle(record.topic(), record.value())
        );


        String beanName = "alertListenerContainer-" + topic;
        if (!context.containsBean(beanName)) {
            context.registerBean(beanName, ConcurrentMessageListenerContainer.class, () -> container);
        }
    }
}

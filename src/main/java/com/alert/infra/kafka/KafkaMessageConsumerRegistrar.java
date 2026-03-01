package com.alert.infra.kafka;

import com.alert.core.messaging.consumer.MessageConsumerRegistrar;
import com.alert.core.messaging.consumer.TopicAlertMessageHandler;
import com.alert.core.messaging.model.AlertMessage;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

public class KafkaMessageConsumerRegistrar implements MessageConsumerRegistrar {
    private final ConcurrentKafkaListenerContainerFactory<String, AlertMessage> kafkaListenerContainerFactory;
    private final GenericApplicationContext context;
    private final CommonErrorHandler errorHandler;

    public KafkaMessageConsumerRegistrar(ConcurrentKafkaListenerContainerFactory<String, AlertMessage> kafkaListenerContainerFactory, ApplicationContext context, CommonErrorHandler errorHandler) {
        this.kafkaListenerContainerFactory = kafkaListenerContainerFactory;
        this.context = (GenericApplicationContext) context;
        this.errorHandler = errorHandler;
    }

    @Override
    public void register(String topic, TopicAlertMessageHandler messageHandler) {
        ConcurrentMessageListenerContainer<String, AlertMessage> container = kafkaListenerContainerFactory.createContainer(topic);
        ContainerProperties containerProperties = container.getContainerProperties();
        if (errorHandler != null) {
            container.setCommonErrorHandler(errorHandler);
        }
        containerProperties.setMessageListener((MessageListener<String, AlertMessage>) record ->
                messageHandler.handle(record.topic(), record.value())
        );


        String beanName = "alertListenerContainer-" + topic;
        if (!context.containsBean(beanName)) {
            context.registerBean(beanName, ConcurrentMessageListenerContainer.class, () -> container);
        }
    }
}

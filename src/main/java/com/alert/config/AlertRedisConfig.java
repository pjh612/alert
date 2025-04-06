package com.alert.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.alert.cache.AlertCacheManager;
import com.alert.cache.RedisAlertCacheManager;
import com.alert.core.messaging.broadcaster.*;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.List;

@Configuration
public class AlertRedisConfig {
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory redisConnectionFactory) {
        RedisMessageListenerContainer redisMessageListenerContainer = new RedisMessageListenerContainer();
        redisMessageListenerContainer.setConnectionFactory(redisConnectionFactory);

        return redisMessageListenerContainer;
    }

    @Bean
    @ConditionalOnMissingBean(AlertCacheManager.class)
    public <T extends AlertMessage> AlertCacheManager<T> alertCacheManager(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
        return new RedisAlertCacheManager<>(redisTemplate, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean(MessageConverter.class)
    public MessageConverter<byte[], ? extends AlertMessage> alertMessageConverter(ObjectMapper objectMapper) {
        return new ByteArrayJsonMessageConverter<>(objectMapper, DefaultAlertMessage.class);
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(redisConnectionFactory);
        return template;
    }

    @Bean
    @ConditionalOnMissingBean(MessageBroadcaster.class)
    public MessageBroadcaster<String> messageBroadcaster(StringRedisTemplate redisTemplate) {
        return new RedisMessageBroadcaster<>(redisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean(MessageListenerRegistrar.class)
    public <T extends AlertMessage> MessageListenerRegistrar<T> messageListenerRegistrar(RedisMessageListenerContainer redisMessageListenerContainer,
                                                                                         AlertProperties alertProperties,
                                                                                         AlertCacheManager<T> alertCacheManager,
                                                                                         AlertMessageSender alertMessageSender,
                                                                                         MessageConverter<byte[], T> alertMessageConverter) {
        RedisMessageListenerRegistrar<T> redisMessageListenerRegistrar = new RedisMessageListenerRegistrar<>(redisMessageListenerContainer);
        List<String> topics = alertProperties.topics();
        for (String topic : topics) {
            redisMessageListenerRegistrar.register(topic, new RedisAlertMessageHandler<>(alertCacheManager, alertMessageSender, topic), alertMessageConverter);
        }

        return redisMessageListenerRegistrar;


    }
}

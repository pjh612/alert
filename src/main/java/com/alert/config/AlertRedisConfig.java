package com.alert.config;

import com.alert.cache.AlertCacheManager;
import com.alert.cache.RedisAlertCacheManager;
import com.alert.core.messaging.broadcaster.*;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    public AlertCacheManager alertCacheManager(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
        return new RedisAlertCacheManager(redisTemplate, objectMapper);
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
    @ConditionalOnMissingBean(AlertMessageListenerRegistrar.class)
    public AlertMessageListenerRegistrar<byte[]> messageListenerRegistrar(RedisMessageListenerContainer redisMessageListenerContainer,
                                                                          AlertProperties alertProperties,
                                                                          AlertCacheManager alertCacheManager,
                                                                          AlertMessageSender alertMessageSender,
                                                                          MessageConverter<byte[], ? extends AlertMessage> alertMessageConverter) {
        RedisMessageListenerRegistrar redisMessageListenerRegistrar = new RedisMessageListenerRegistrar(redisMessageListenerContainer);
        List<String> topics = alertProperties.topics();
        for (String topic : topics) {
            redisMessageListenerRegistrar.register(topic, new DefaultAlertMessageHandler(alertCacheManager, alertMessageSender, topic), alertMessageConverter);
        }

        return redisMessageListenerRegistrar;


    }
}

package com.alert.config;

import com.alert.cache.AlertCacheManager;
import com.alert.cache.ReactiveAlertCacheManager;
import com.alert.cache.ReactiveRedisAlertCacheManager;
import com.alert.cache.RedisAlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageListenerRegistrar;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.broadcaster.ByteArrayJsonMessageConverter;
import com.alert.core.messaging.broadcaster.DefaultAlertMessageHandler;
import com.alert.core.messaging.broadcaster.DefaultReactiveAlertMessageHandler;
import com.alert.core.messaging.broadcaster.MessageBroadcaster;
import com.alert.core.messaging.broadcaster.MessageConverter;
import com.alert.core.messaging.broadcaster.ReactiveAlertMessageListenerRegistrar;
import com.alert.core.messaging.broadcaster.ReactiveMessageBroadcaster;
import com.alert.core.messaging.broadcaster.ReactiveRedisMessageBroadcaster;
import com.alert.core.messaging.broadcaster.ReactiveRedisMessageListenerRegistrar;
import com.alert.core.messaging.broadcaster.RedisMessageBroadcaster;
import com.alert.core.messaging.broadcaster.RedisMessageListenerRegistrar;
import com.alert.core.messaging.broadcaster.StringJsonMessageConverter;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

@Configuration
public class AlertRedisConfig {

    @Bean
    @ConditionalOnMissingBean(RedisMessageListenerContainer.class)
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory redisConnectionFactory) {
        RedisMessageListenerContainer redisMessageListenerContainer = new RedisMessageListenerContainer();
        redisMessageListenerContainer.setConnectionFactory(redisConnectionFactory);
        return redisMessageListenerContainer;
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(ReactiveRedisMessageListenerContainer.class)
    public ReactiveRedisMessageListenerContainer reactiveRedisMessageListenerContainer(LettuceConnectionFactory redisConnectionFactory) {
        return new ReactiveRedisMessageListenerContainer(redisConnectionFactory);
    }

    @Bean
    @ConditionalOnMissingBean(AlertCacheManager.class)
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    public AlertCacheManager alertCacheManager(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
        return new RedisAlertCacheManager(redisTemplate, objectMapper);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(ReactiveAlertCacheManager.class)
    public ReactiveAlertCacheManager reactiveAlertCacheManager(ReactiveRedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
        return new ReactiveRedisAlertCacheManager(redisTemplate, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean(MessageConverter.class)
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    public MessageConverter<byte[], ? extends AlertMessage> alertMessageConverter(ObjectMapper objectMapper) {
        return new ByteArrayJsonMessageConverter<>(objectMapper, DefaultAlertMessage.class);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(MessageConverter.class)
    public MessageConverter<String, ? extends AlertMessage> reactiveAlertMessageConverter(ObjectMapper objectMapper) {
        return new StringJsonMessageConverter<>(objectMapper, DefaultAlertMessage.class);
    }


    @Bean
    @ConditionalOnMissingBean(StringRedisTemplate.class)
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(redisConnectionFactory);
        return template;
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(ReactiveStringRedisTemplate.class)
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(ReactiveRedisConnectionFactory redisConnectionFactory) {
        return new ReactiveStringRedisTemplate(redisConnectionFactory);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    @ConditionalOnMissingBean(MessageBroadcaster.class)
    public MessageBroadcaster<String> messageBroadcaster(StringRedisTemplate redisTemplate) {
        return new RedisMessageBroadcaster<>(redisTemplate);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(ReactiveMessageBroadcaster.class)
    public ReactiveMessageBroadcaster<String, Long> reactiveMessageBroadcaster(ReactiveRedisTemplate<String, String> redisTemplate) {
        return new ReactiveRedisMessageBroadcaster<>(redisTemplate);
    }


    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    @ConditionalOnMissingBean(AlertMessageListenerRegistrar.class)
    public AlertMessageListenerRegistrar<byte[]> messageListenerRegistrar(
            RedisMessageListenerContainer redisMessageListenerContainer,
            AlertProperties alertProperties,
            AlertCacheManager alertCacheManager,
            AlertMessageSender alertMessageSender,
            AlertMessageSupport alertMessageSupport,
            MessageConverter<byte[], ? extends AlertMessage> alertMessageConverter) {

        RedisMessageListenerRegistrar redisMessageListenerRegistrar = new RedisMessageListenerRegistrar(redisMessageListenerContainer);

        List<String> topics = alertProperties.topics();
        for (String topic : topics) {
            redisMessageListenerRegistrar.register(
                    topic,
                    new DefaultAlertMessageHandler(alertCacheManager, alertMessageSender, alertMessageSupport, topic),
                    alertMessageConverter
            );
        }
        return redisMessageListenerRegistrar;
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(ReactiveAlertMessageListenerRegistrar.class)
    public ReactiveAlertMessageListenerRegistrar<String> reactiveMessageListenerRegistrar(ReactiveRedisMessageListenerContainer redisMessageListenerContainer,
                                                                                          ReactiveAlertCacheManager alertCacheManager,
                                                                                          AlertMessageSender alertMessageSender,
                                                                                          MessageConverter<String, ? extends AlertMessage> alertMessageConverter,
                                                                                          AlertMessageSupport alertMessageSupport,
                                                                                          AlertProperties alertProperties) {
        ReactiveRedisMessageListenerRegistrar redisMessageListenerRegistrar = new ReactiveRedisMessageListenerRegistrar(redisMessageListenerContainer);
        List<String> topics = alertProperties.topics();
        for (String topic : topics) {
            redisMessageListenerRegistrar.register(topic, new DefaultReactiveAlertMessageHandler(alertCacheManager, alertMessageSender, alertMessageSupport, topic), alertMessageConverter);
        }

        return redisMessageListenerRegistrar;
    }
}


package com.alert.config;

import com.alert.cache.AlertCacheManager;
import com.alert.cache.ReactiveAlertCacheManager;
import com.alert.core.manager.AlertManager;
import com.alert.core.manager.ReactiveSubscribableAlertManager;
import com.alert.core.manager.SubscribableAlertManager;
import com.alert.core.messaging.bridge.AlertMessageBroadcastHandler;
import com.alert.core.messaging.bridge.AlertMessagePublisher;
import com.alert.core.messaging.bridge.ReactiveAlertMessagePublisher;
import com.alert.core.messaging.bridge.TopicAlertMessageHandler;
import com.alert.core.messaging.broadcaster.MessageBroadcaster;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageFactory;
import com.alert.core.messaging.sender.AlertMessageSender;
import com.alert.sse.EmitterRepository;
import com.alert.sse.ReactiveEmitterRepository;
import com.alert.sse.ReactiveSseAlertManager;
import com.alert.sse.ReactiveSseAlertMessageSender;
import com.alert.sse.SseAlertManager;
import com.alert.sse.SseAlertMessageSender;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;
import tools.jackson.databind.ObjectMapper;

@EnableConfigurationProperties(AlertProperties.class)
@Import({EmitterRepositoryConfig.class, AlertRedisConfig.class, AlertKafkaConfig.class})
@Configuration
public class AlertConfig {
    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    @ConditionalOnMissingBean(AlertManager.class)
    public SubscribableAlertManager<SseEmitter> defaultAlertManager(
            EmitterRepository emitterRepository,
            AlertCacheManager alertCacheManager,
            AlertMessageFactory alertMessageConverter,
            AlertMessagePublisher alertMessagePublisher
    ) {
        return new SseAlertManager(alertMessagePublisher, alertMessageConverter, emitterRepository, alertCacheManager, DefaultAlertMessage.class);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    public ReactiveSubscribableAlertManager<Flux<ServerSentEvent<Object>>> reactiveDefaultAlertManager(
            ReactiveEmitterRepository emitterRepository,
            ReactiveAlertCacheManager alertCacheManager,
            AlertMessageFactory alertMessageConverter,
            ReactiveAlertMessagePublisher alertMessagePublisher
    ) {
        return new ReactiveSseAlertManager(alertMessagePublisher, alertMessageConverter, emitterRepository, alertCacheManager, DefaultAlertMessage.class);
    }

    @Bean
    @ConditionalOnMissingBean(AlertMessageFactory.class)
    public AlertMessageFactory alertMessageFactory() {
        return new DefaultAlertMessageFactory();
    }


    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    @ConditionalOnMissingBean(AlertMessageSender.class)
    public AlertMessageSender sseAlertMessageSender(EmitterRepository emitterRepository) {
        return new SseAlertMessageSender(emitterRepository);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(AlertMessageSender.class)
    public AlertMessageSender reactiveSseAlertMessageSender(ReactiveEmitterRepository emitterRepository) {
        return new ReactiveSseAlertMessageSender(emitterRepository);
    }

    @Bean
    TopicAlertMessageHandler messageHandler(MessageBroadcaster<String> messageBroadcaster, ObjectMapper objectMapper) {
        return new AlertMessageBroadcastHandler(messageBroadcaster, objectMapper);
    }
}
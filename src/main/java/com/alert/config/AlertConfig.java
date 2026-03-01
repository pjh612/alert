package com.alert.config;

import com.alert.core.cache.AlertCacheManager;
import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.manager.AlertManager;
import com.alert.core.manager.ReactiveSubscribableAlertManager;
import com.alert.core.manager.SubscribableAlertManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.broadcaster.MessageBroadcaster;
import com.alert.core.messaging.broadcaster.ReactiveMessageBroadcaster;
import com.alert.core.messaging.consumer.AlertMessageBroadcastHandler;
import com.alert.core.messaging.consumer.ReactiveAlertMessageBroadcastHandler;
import com.alert.core.messaging.consumer.ReactiveTopicAlertMessageHandler;
import com.alert.core.messaging.consumer.TopicAlertMessageHandler;
import com.alert.core.messaging.id.IdGenerator;
import com.alert.core.messaging.id.SnowflakeIdGenerator;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageFactory;
import com.alert.core.messaging.publisher.AlertMessagePublisher;
import com.alert.core.messaging.publisher.ReactiveAlertMessagePublisher;
import com.alert.core.messaging.sender.BasicAlertMessageDelegateSender;
import com.alert.core.messaging.sender.BasicAlertMessageSender;
import com.alert.core.messaging.sender.FanoutAlertMessageSender;
import com.alert.core.session.TagBasedAlertSessionRepository;
import com.alert.sse.ReactiveSseAlertManager;
import com.alert.sse.ReactiveSseAlertMessageSender;
import com.alert.sse.SseAlertManager;
import com.alert.sse.SseAlertMessageSender;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import tools.jackson.databind.ObjectMapper;

@EnableConfigurationProperties(AlertProperties.class)
@Import({EmitterRepositoryConfig.class, AlertRedisConfig.class, AlertKafkaConfig.class})
@Configuration
public class AlertConfig {

    private final AlertProperties properties;

    public AlertConfig(AlertProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    @ConditionalOnMissingBean(AlertManager.class)
    public SubscribableAlertManager<SseEmitter> defaultAlertManager(
            TagBasedAlertSessionRepository<SseEmitter> emitterRepository,
            AlertCacheManager alertCacheManager,
            AlertMessageFactory alertMessageConverter,
            AlertMessagePublisher alertMessagePublisher,
            AlertMessageSupport alertMessageSupport
    ) {
        return new SseAlertManager(alertMessagePublisher, alertMessageConverter, emitterRepository, alertCacheManager, alertMessageSupport, DefaultAlertMessage.class);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    public ReactiveSubscribableAlertManager<Flux<ServerSentEvent<Object>>> reactiveDefaultAlertManager(
            TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> emitterRepository,
            ReactiveAlertCacheManager alertCacheManager,
            AlertMessageFactory alertMessageConverter,
            ReactiveAlertMessagePublisher alertMessagePublisher,
            AlertMessageSupport alertMessageSupport
    ) {
        return new ReactiveSseAlertManager(alertMessagePublisher, alertMessageConverter, emitterRepository, alertCacheManager, alertMessageSupport, DefaultAlertMessage.class);
    }

    @Bean
    @ConditionalOnMissingBean(AlertMessageFactory.class)
    public AlertMessageFactory alertMessageFactory(AlertMessageSupport alertMessageSupport) {
        return new DefaultAlertMessageFactory(alertMessageSupport);
    }


    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    @ConditionalOnMissingBean(FanoutAlertMessageSender.class)
    public FanoutAlertMessageSender sseAlertMessageSender(TagBasedAlertSessionRepository<SseEmitter> emitterRepository) {
        return new SseAlertMessageSender(emitterRepository);
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(FanoutAlertMessageSender.class)
    public FanoutAlertMessageSender reactiveSseAlertMessageSender(TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> emitterRepository) {
        return new ReactiveSseAlertMessageSender(emitterRepository);
    }

    @Bean
    @ConditionalOnMissingBean(TopicAlertMessageHandler.class)
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    TopicAlertMessageHandler topicAlertMessageHandler(MessageBroadcaster<String> messageBroadcaster, ObjectMapper objectMapper,
                                                      ObjectProvider<BasicAlertMessageSender> basicSenderProvider) {
        return new AlertMessageBroadcastHandler(messageBroadcaster, objectMapper, resolveBasicSender(basicSenderProvider));
    }

    @Bean
    @ConditionalOnMissingBean(ReactiveTopicAlertMessageHandler.class)
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    ReactiveTopicAlertMessageHandler<Long> reactiveTopicAlertMessageHandler(ReactiveMessageBroadcaster<String, Long> messageBroadcaster, ObjectMapper objectMapper,
                                                                            ObjectProvider<BasicAlertMessageSender> basicSenderProvider) {
        return new ReactiveAlertMessageBroadcastHandler<>(messageBroadcaster, objectMapper, resolveBasicSender(basicSenderProvider));
    }

    private BasicAlertMessageSender resolveBasicSender(ObjectProvider<BasicAlertMessageSender> provider) {
        List<BasicAlertMessageSender> senders = provider.stream().toList();
        if (senders.isEmpty()) return null;
        if (senders.size() == 1) return senders.get(0);
        return new BasicAlertMessageDelegateSender(senders);
    }

    @Bean
    @ConditionalOnMissingBean(AlertMessageSupport.class)
    public AlertMessageSupport alertMessageSupport(IdGenerator<?> idGenerator) {
        return new AlertMessageSupport(idGenerator);
    }

    @Bean
    @ConditionalOnMissingBean(IdGenerator.class)
    public IdGenerator<?> idGenerator() {
        return new SnowflakeIdGenerator(properties.nodeId());
    }
}
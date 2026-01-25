package com.alert.config;

import com.alert.sse.AlertSessionRepository;
import com.alert.sse.InMemoryAlertSessionRepository;
import com.alert.sse.InMemoryTagBasedAlertSessionRepository;
import com.alert.sse.ReactiveEmitterRepository;
import com.alert.sse.ReactiveInMemoryEmitterRepository;
import com.alert.sse.TagBasedAlertSessionRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Sinks;

@Configuration
public class EmitterRepositoryConfig {
    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    @ConditionalOnMissingBean(AlertSessionRepository.class)
    public TagBasedAlertSessionRepository<SseEmitter> inMemoryEmitterRepository() {
        return new InMemoryTagBasedAlertSessionRepository<>();
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(AlertSessionRepository.class)
    public TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> reactiveInMemoryEmitterRepository() {
        return new ReactiveEmitterRepository();
    }
}

package com.alert.config;

import com.alert.core.session.AlertSessionRepository;
import com.alert.core.session.InMemoryTagBasedAlertSessionRepository;
import com.alert.core.session.TagBasedAlertSessionRepository;
import com.alert.infra.micrometer.MeteredAlertSessionRepository;
import com.alert.sse.ReactiveEmitterRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
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
    public TagBasedAlertSessionRepository<SseEmitter> inMemoryEmitterRepository(
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        InMemoryTagBasedAlertSessionRepository<SseEmitter> repo = new InMemoryTagBasedAlertSessionRepository<>();
        MeterRegistry meterRegistry = meterRegistryProvider.getIfAvailable();
        if (meterRegistry != null) {
            return new MeteredAlertSessionRepository<>(repo, meterRegistry);
        }
        return repo;
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(AlertSessionRepository.class)
    public TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> reactiveInMemoryEmitterRepository(
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        ReactiveEmitterRepository repo = new ReactiveEmitterRepository();
        MeterRegistry meterRegistry = meterRegistryProvider.getIfAvailable();
        if (meterRegistry != null) {
            return new MeteredAlertSessionRepository<>(repo, meterRegistry);
        }
        return repo;
    }
}

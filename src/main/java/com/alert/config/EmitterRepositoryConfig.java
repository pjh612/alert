package com.alert.config;

import com.alert.sse.EmitterRepository;
import com.alert.sse.InMemoryEmitterRepository;
import com.alert.sse.ReactiveEmitterRepository;
import com.alert.sse.ReactiveInMemoryEmitterRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EmitterRepositoryConfig {
    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "false", matchIfMissing = true)
    @ConditionalOnMissingBean(EmitterRepository.class)
    public EmitterRepository inMemoryEmitterRepository() {
        return new InMemoryEmitterRepository();
    }

    @Bean
    @ConditionalOnProperty(name = "alert.reactive", havingValue = "true")
    @ConditionalOnMissingBean(ReactiveEmitterRepository.class)
    public ReactiveEmitterRepository reactiveInMemoryEmitterRepository() {
        return new ReactiveInMemoryEmitterRepository();
    }
}

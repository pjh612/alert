package com.alert.config;

import com.alert.sse.EmitterRepository;
import com.alert.sse.InMemoryEmitterRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EmitterRepositoryConfig {
    @Bean
    @ConditionalOnMissingBean(EmitterRepository.class)
    public EmitterRepository inMemoryEmitterRepository() {
        return new InMemoryEmitterRepository();
    }
}

package com.alert.sse;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Optional;

public interface EmitterRepository {
    void put(String id, SseEmitter emitter);

    Optional<SseEmitter> getById(String id);

    SseEmitter deleteById(String id);
}

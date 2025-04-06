package com.alert.sse;

import org.springframework.stereotype.Repository;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class InMemoryEmitterRepository implements EmitterRepository {
    private final Map<String, SseEmitter> emitterMap;

    public InMemoryEmitterRepository() {
        this.emitterMap = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String id, SseEmitter emitter) {
        emitterMap.put(id, emitter);
    }

    @Override
    public Optional<SseEmitter> getById(String id) {
        return Optional.ofNullable(emitterMap.get(id));
    }

    @Override
    public SseEmitter deleteById(String id) {
        return emitterMap.remove(id);
    }
}

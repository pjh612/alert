package com.alert.sse;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReactiveInMemoryEmitterRepository implements ReactiveEmitterRepository {
    private final Map<String, Sinks.Many<ServerSentEvent<Object>>> emitterMap = new ConcurrentHashMap<>();

    @Override
    public Sinks.Many<ServerSentEvent<Object>> put(String id) {
        Sinks.Many<ServerSentEvent<Object>> sink = Sinks.many().multicast().onBackpressureBuffer();
        this.emitterMap.put(id, sink);

        return sink;
    }

    @Override
    public Sinks.Many<ServerSentEvent<Object>> getById(String id) {
        return this.emitterMap.get(id);
    }

    @Override
    public void deleteById(String id) {
        Sinks.Many<ServerSentEvent<Object>> sink = this.emitterMap.remove(id);
        if (sink != null) {
            sink.tryEmitComplete();
        }
    }
}

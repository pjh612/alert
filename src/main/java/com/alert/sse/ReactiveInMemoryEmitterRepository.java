package com.alert.sse;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ReactiveInMemoryEmitterRepository implements AlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> {
    private final Map<String, AlertSession<Sinks.Many<ServerSentEvent<Object>>>> sessionMap = new ConcurrentHashMap<>();

    @Override
    public void put(String id, Sinks.Many<ServerSentEvent<Object>> engine) {
        sessionMap.put(id, new AlertSession<>(id, engine, null));
    }

    @Override
    public Optional<AlertSession<Sinks.Many<ServerSentEvent<Object>>>> getById(String id) {
        return Optional.ofNullable(sessionMap.get(id));
    }

    @Override
    public AlertSession<Sinks.Many<ServerSentEvent<Object>>> deleteById(String id) {
        AlertSession<Sinks.Many<ServerSentEvent<Object>>> session = sessionMap.remove(id);

        if (session != null && session.engine() != null) {
            session.engine().tryEmitComplete();
        }

        return session;
    }

    @Override
    public long size() {
        return sessionMap.values()
                .stream()
                .filter(it -> it.engine().currentSubscriberCount() > 0)
                .count();
    }
}

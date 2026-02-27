package com.alert.sse;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ReactiveInMemoryEmitterRepository implements AlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> {
    // namespace -> id -> session
    private final Map<String, Map<String, AlertSession<Sinks.Many<ServerSentEvent<Object>>>>> sessionMap = new ConcurrentHashMap<>();

    @Override
    public void put(String namespace, String id, Sinks.Many<ServerSentEvent<Object>> engine) {
        sessionMap.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>())
                .put(id, new AlertSession<>(id, engine, null));
    }

    @Override
    public Optional<AlertSession<Sinks.Many<ServerSentEvent<Object>>>> getById(String namespace, String id) {
        Map<String, AlertSession<Sinks.Many<ServerSentEvent<Object>>>> ns = sessionMap.get(namespace);
        if (ns == null) return Optional.empty();
        return Optional.ofNullable(ns.get(id));
    }

    @Override
    public List<AlertSession<Sinks.Many<ServerSentEvent<Object>>>> getAll(String namespace) {
        Map<String, AlertSession<Sinks.Many<ServerSentEvent<Object>>>> ns = sessionMap.get(namespace);
        if (ns == null) return Collections.emptyList();
        return List.copyOf(ns.values());
    }

    @Override
    public AlertSession<Sinks.Many<ServerSentEvent<Object>>> deleteById(String namespace, String id) {
        Map<String, AlertSession<Sinks.Many<ServerSentEvent<Object>>>> ns = sessionMap.get(namespace);
        if (ns == null) return null;

        AlertSession<Sinks.Many<ServerSentEvent<Object>>> session = ns.remove(id);
        if (session != null && session.engine() != null) {
            session.engine().tryEmitComplete();
        }
        return session;
    }

    @Override
    public long size() {
        return sessionMap.values().stream()
                .flatMap(ns -> ns.values().stream())
                .filter(it -> it.engine().currentSubscriberCount() > 0)
                .count();
    }

    @Override
    public long size(String namespace) {
        Map<String, AlertSession<Sinks.Many<ServerSentEvent<Object>>>> ns = sessionMap.get(namespace);
        if (ns == null) return 0;
        return ns.values().stream()
                .filter(it -> it.engine().currentSubscriberCount() > 0)
                .count();
    }
}

package com.alert.core.session;

import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class InMemoryAlertSessionRepository<T> implements AlertSessionRepository<T> {
    private final Map<String, Map<String, AlertSession<T>>> sessionMap;

    public InMemoryAlertSessionRepository() {
        this.sessionMap = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String namespace, String id, T engine) {
        sessionMap.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>())
                .put(id, new AlertSession<>(id, engine, null));
    }

    @Override
    public Optional<AlertSession<T>> getById(String namespace, String id) {
        Map<String, AlertSession<T>> ns = sessionMap.get(namespace);
        if (ns == null) return Optional.empty();
        return Optional.ofNullable(ns.get(id));
    }

    @Override
    public List<AlertSession<T>> getAll(String namespace) {
        Map<String, AlertSession<T>> ns = sessionMap.get(namespace);
        if (ns == null) return Collections.emptyList();
        return List.copyOf(ns.values());
    }

    @Override
    public AlertSession<T> deleteById(String namespace, String id) {
        Map<String, AlertSession<T>> ns = sessionMap.get(namespace);
        if (ns == null) return null;
        return ns.remove(id);
    }

    @Override
    public long size() {
        return sessionMap.values().stream()
                .mapToLong(Map::size)
                .sum();
    }

    @Override
    public long size(String namespace) {
        Map<String, AlertSession<T>> ns = sessionMap.get(namespace);
        return ns == null ? 0 : ns.size();
    }
}

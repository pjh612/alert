package com.alert.sse;

import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class InMemoryAlertSessionRepository<T> implements AlertSessionRepository<T> {
    private final Map<String, AlertSession<T>> sessionMap;

    public InMemoryAlertSessionRepository() {
        this.sessionMap = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String id, T engine) {
        sessionMap.put(id, new AlertSession<>(id, engine, null));
    }

    @Override
    public Optional<AlertSession<T>> getById(String id) {
        return Optional.ofNullable(sessionMap.get(id));
    }

    @Override
    public AlertSession<T> deleteById(String id) {
        return sessionMap.remove(id);
    }

    @Override
    public long size() {
        return sessionMap.size();
    }
}

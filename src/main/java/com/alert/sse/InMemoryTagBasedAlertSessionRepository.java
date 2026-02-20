package com.alert.sse;

import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Repository
public class InMemoryTagBasedAlertSessionRepository<T> implements TagBasedAlertSessionRepository<T> {
    private final Map<String, AlertSession<T>> sessionMap;
    private final Map<String, Set<String>> tagIndex;

    public InMemoryTagBasedAlertSessionRepository() {
        this.sessionMap = new ConcurrentHashMap<>();
        this.tagIndex = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String id, T emitter) {
        if (sessionMap.containsKey(id)) {
            deleteById(id);
        }

        AlertSession<T> sseSession = new AlertSession<>(id, emitter, new CopyOnWriteArraySet<>());
        sessionMap.put(id, sseSession);
    }

    @Override
    public void put(String id, Set<String> tags, T engine) {
        if (sessionMap.containsKey(id)) {
            deleteById(id);
        }

        Set<String> safeTags = (tags == null) ? new CopyOnWriteArraySet<>() : new CopyOnWriteArraySet<>(tags);
        AlertSession<T> sseSession = new AlertSession<>(id, engine, safeTags);

        for (String tag : safeTags) {
            tagIndex.computeIfAbsent(tag, k -> new CopyOnWriteArraySet<>()).add(id);
        }

        sessionMap.put(id, sseSession);
    }

    @Override
    public void addTag(String id, String tag) {
        AlertSession<T> sseSession = sessionMap.get(id);
        if (sseSession != null) {
            sseSession.addTag(tag);
            tagIndex.computeIfAbsent(tag, k -> new CopyOnWriteArraySet<>()).add(id);
        }
    }


    @Override
    public List<AlertSession<T>> getByTag(String tag) {

        Set<String> ids = tagIndex.get(tag);
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }

        return ids.stream()
                .map(sessionMap::get)
                .filter(Objects::nonNull)
                .toList();
    }

    @Override
    public void deleteByTag(String tag) {
        Set<String> ids = tagIndex.get(tag);
        if (ids != null) {
            new HashSet<>(ids).forEach(this::deleteById);
        }
    }

    @Override
    public Optional<AlertSession<T>> getById(String id) {
        AlertSession<T> sseSession = sessionMap.get(id);
        if (sseSession != null) {
            return Optional.of(sseSession);
        }
        return Optional.empty();
    }

    @Override
    public AlertSession<T> deleteById(String id) {
        AlertSession<T> sseSession = sessionMap.remove(id);
        if (sseSession == null) {
            return null;
        }

        sseSession.tags().forEach(tag -> {
            tagIndex.computeIfPresent(tag, (k, ids) -> {
                ids.remove(id);
                return ids.isEmpty() ? null : ids;
            });
        });

        return sseSession;
    }

    @Override
    public long size() {
        return sessionMap.size();
    }

}

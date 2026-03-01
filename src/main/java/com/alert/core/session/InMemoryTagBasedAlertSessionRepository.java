package com.alert.core.session;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class InMemoryTagBasedAlertSessionRepository<T> implements TagBasedAlertSessionRepository<T> {
    private final Map<String, Map<String, AlertSession<T>>> sessionMap;
    private final Map<String, Map<String, Set<String>>> tagIndex;

    public InMemoryTagBasedAlertSessionRepository() {
        this.sessionMap = new ConcurrentHashMap<>();
        this.tagIndex = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String namespace, String id, T engine) {
        Map<String, AlertSession<T>> ns = sessionMap.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>());
        if (ns.containsKey(id)) {
            deleteById(namespace, id);
        }
        ns.put(id, new AlertSession<>(id, engine, new CopyOnWriteArraySet<>()));
    }

    @Override
    public void put(String namespace, String id, Set<String> tags, T engine) {
        Map<String, AlertSession<T>> ns = sessionMap.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>());
        if (ns.containsKey(id)) {
            deleteById(namespace, id);
        }

        Set<String> safeTags = (tags == null) ? new CopyOnWriteArraySet<>() : new CopyOnWriteArraySet<>(tags);
        AlertSession<T> session = new AlertSession<>(id, engine, safeTags);

        Map<String, Set<String>> nsTagIndex = tagIndex.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>());
        for (String tag : safeTags) {
            nsTagIndex.computeIfAbsent(tag, k -> new CopyOnWriteArraySet<>()).add(id);
        }

        ns.put(id, session);
    }

    @Override
    public void addTag(String namespace, String id, String tag) {
        Map<String, AlertSession<T>> ns = sessionMap.get(namespace);
        if (ns == null) return;

        AlertSession<T> session = ns.get(id);
        if (session != null) {
            session.addTag(tag);
            tagIndex.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(tag, k -> new CopyOnWriteArraySet<>())
                    .add(id);
        }
    }

    @Override
    public List<AlertSession<T>> getByTag(String namespace, String tag) {
        Map<String, Set<String>> nsTagIndex = tagIndex.get(namespace);
        if (nsTagIndex == null) return Collections.emptyList();

        Set<String> ids = nsTagIndex.get(tag);
        if (ids == null || ids.isEmpty()) return Collections.emptyList();

        Map<String, AlertSession<T>> ns = sessionMap.get(namespace);
        if (ns == null) return Collections.emptyList();

        return ids.stream()
                .map(ns::get)
                .filter(Objects::nonNull)
                .toList();
    }

    @Override
    public void deleteByTag(String namespace, String tag) {
        Map<String, Set<String>> nsTagIndex = tagIndex.get(namespace);
        if (nsTagIndex == null) return;

        Set<String> ids = nsTagIndex.get(tag);
        if (ids != null) {
            new HashSet<>(ids).forEach(id -> deleteById(namespace, id));
        }
    }

    @Override
    public List<AlertSession<T>> getAll(String namespace) {
        Map<String, AlertSession<T>> ns = sessionMap.get(namespace);
        if (ns == null) return Collections.emptyList();
        return List.copyOf(ns.values());
    }

    @Override
    public Optional<AlertSession<T>> getById(String namespace, String id) {
        Map<String, AlertSession<T>> ns = sessionMap.get(namespace);
        if (ns == null) return Optional.empty();
        return Optional.ofNullable(ns.get(id));
    }

    @Override
    public AlertSession<T> deleteById(String namespace, String id) {
        Map<String, AlertSession<T>> ns = sessionMap.get(namespace);
        if (ns == null) return null;

        AlertSession<T> session = ns.remove(id);
        if (session == null) return null;

        Map<String, Set<String>> nsTagIndex = tagIndex.get(namespace);
        if (nsTagIndex != null) {
            session.tags().forEach(tag -> nsTagIndex.computeIfPresent(tag, (k, ids) -> {
                ids.remove(id);
                return ids.isEmpty() ? null : ids;
            }));
        }

        return session;
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

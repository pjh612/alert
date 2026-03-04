package com.alert.core.session;

import java.util.List;
import java.util.Optional;

public interface AlertSessionRepository<T> {
    AlertSession<T> put(String namespace, String id, T engine);

    Optional<AlertSession<T>> getById(String namespace, String id);

    List<AlertSession<T>> getAll(String namespace);

    AlertSession<T> deleteById(String namespace, String id);

    long size();

    long size(String namespace);
}

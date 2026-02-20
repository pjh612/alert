package com.alert.sse;

import java.util.Optional;

public interface AlertSessionRepository<T> {
    void put(String id, T engine);

    Optional<AlertSession<T>> getById(String id);

    AlertSession<T> deleteById(String id);

    long size();
}

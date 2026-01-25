package com.alert.sse;

import java.util.List;
import java.util.Set;

public interface TagBasedAlertSessionRepository<T> extends AlertSessionRepository<T> {
    void put(String id, Set<String> tags, T engine);

    void addTag(String id, String tag);

    List<AlertSession<T>> getByTag(String tag);

    void deleteByTag(String id);
}

package com.alert.core.session;

import java.util.List;
import java.util.Set;

public interface TagBasedAlertSessionRepository<T> extends AlertSessionRepository<T> {
    void put(String namespace, String id, Set<String> tags, T engine);

    void addTag(String namespace, String id, String tag);

    List<AlertSession<T>> getByTag(String namespace, String tag);

    void deleteByTag(String namespace, String tag);
}

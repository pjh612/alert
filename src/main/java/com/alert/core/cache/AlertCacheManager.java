package com.alert.core.cache;

import com.alert.core.messaging.model.AlertMessage;

import java.util.List;

public interface AlertCacheManager {
    Boolean save(String key, String id, AlertMessage value);

    default void saveAll(List<String> keys, String id, AlertMessage value) {
        keys.forEach(key -> save(key, id, value));
    }

    List<? extends AlertMessage> getFromOffset(String key, String offset, Class<? extends AlertMessage> tClass);
}

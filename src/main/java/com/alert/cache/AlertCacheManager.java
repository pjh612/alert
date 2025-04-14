package com.alert.cache;

import com.alert.core.messaging.model.AlertMessage;

import java.util.List;

public interface AlertCacheManager {
    Boolean save(String key, String id, AlertMessage value);

    List<? extends AlertMessage> getFromOffset(String key, Long offset, Class<? extends AlertMessage> tClass);
}

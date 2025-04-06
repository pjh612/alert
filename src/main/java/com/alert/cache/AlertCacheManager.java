package com.alert.cache;

import java.util.List;

public interface AlertCacheManager<T> {
    Boolean save(String key, String id, T value);

    List<T> getFromOffset(String key, Long offset, Class<T> tClass);
}

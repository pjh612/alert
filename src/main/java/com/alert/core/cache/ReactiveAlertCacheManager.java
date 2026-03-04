package com.alert.core.cache;

import com.alert.core.messaging.model.AlertMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveAlertCacheManager {
    Mono<Boolean> save(String key, String id, AlertMessage value);

    Flux<? extends AlertMessage> getFromOffset(String key, String offset, Class<? extends AlertMessage> tClass);
}

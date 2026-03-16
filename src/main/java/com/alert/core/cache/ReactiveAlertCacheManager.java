package com.alert.core.cache;

import com.alert.core.messaging.model.AlertMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface ReactiveAlertCacheManager {
    Mono<Boolean> save(String key, String id, AlertMessage value);

    default Mono<Void> saveAll(List<String> keys, String id, AlertMessage value) {
        return Flux.fromIterable(keys)
                .flatMap(key -> save(key, id, value))
                .then();
    }

    Flux<? extends AlertMessage> getFromOffset(String key, String offset, Class<? extends AlertMessage> tClass);
}

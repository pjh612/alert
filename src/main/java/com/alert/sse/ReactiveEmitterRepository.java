package com.alert.sse;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

public interface ReactiveEmitterRepository {
    Sinks.Many<ServerSentEvent<Object>> put(String id);

    Sinks.Many<ServerSentEvent<Object>> getById(String id);

    void deleteById(String id);
}

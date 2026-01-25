package com.alert.sse;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

public class ReactiveEmitterRepository extends InMemoryTagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> {
    @Override
    public AlertSession<Sinks.Many<ServerSentEvent<Object>>> deleteById(String id) {
        AlertSession<Sinks.Many<ServerSentEvent<Object>>> session = super.deleteById(id);
        if (session != null) {
            session.engine().tryEmitComplete(); // 리액티브는 명시적 종료가 필수!
        }
        return session;
    }
}

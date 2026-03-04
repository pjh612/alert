package com.alert.sse;

import com.alert.core.session.AlertSession;
import com.alert.core.session.InMemoryTagBasedAlertSessionRepository;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

public class ReactiveEmitterRepository extends InMemoryTagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> {
    @Override
    public AlertSession<Sinks.Many<ServerSentEvent<Object>>> deleteById(String namespace, String id) {
        AlertSession<Sinks.Many<ServerSentEvent<Object>>> session = super.deleteById(namespace, id);
        if (session != null) {
            session.engine().tryEmitComplete();
        }
        return session;
    }
}

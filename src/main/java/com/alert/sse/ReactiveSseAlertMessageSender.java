package com.alert.sse;

import com.alert.core.messaging.model.AlertMessage;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

public class ReactiveSseAlertMessageSender extends AbstractAlertMessageSender<Sinks.Many<ServerSentEvent<Object>>> {

    public ReactiveSseAlertMessageSender(TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> repository) {
        super(repository);
    }

    @Override
    protected void doSend(Sinks.Many<ServerSentEvent<Object>> engine, String id, AlertMessage message) {
        engine.tryEmitNext(ServerSentEvent.builder()
                .event(message.type().toString())
                .data(message.body())
                .id(id)
                .build());
    }
}

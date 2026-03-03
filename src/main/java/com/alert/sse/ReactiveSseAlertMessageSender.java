package com.alert.sse;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AbstractAlertMessageSender;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

public class ReactiveSseAlertMessageSender extends AbstractAlertMessageSender<Sinks.Many<ServerSentEvent<Object>>> {

    private static final Logger log = LoggerFactory.getLogger(ReactiveSseAlertMessageSender.class);

    public ReactiveSseAlertMessageSender(TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> repository) {
        super(repository);
    }

    @Override
    protected void doSend(Sinks.Many<ServerSentEvent<Object>> engine, String id, AlertMessage message) {
        Sinks.EmitResult result = engine.tryEmitNext(ServerSentEvent.builder()
                .event(message.type().toString())
                .data(message.body())
                .id(id)
                .build());


        if (result.isFailure()) {
            log.warn("SSE emit failed. ns={}, id={}, type={}, result={}",
                    message.namespace(), id, message.type(), result);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("SSE emit success. ns={}, id={}, type={}",
                    message.namespace(), id, message.type());
        }
    }
}

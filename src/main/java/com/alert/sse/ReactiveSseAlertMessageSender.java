package com.alert.sse;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

public class ReactiveSseAlertMessageSender implements AlertMessageSender {
    private final ReactiveEmitterRepository emitterRepository;

    public ReactiveSseAlertMessageSender(ReactiveEmitterRepository emitterRepository) {
        this.emitterRepository = emitterRepository;
    }

    @Override
    public void send(String id, AlertMessage message) {
        Sinks.Many<ServerSentEvent<Object>> emitter = emitterRepository.getById(message.targetId());
        if (emitter == null) {
            return;
        }
        emitter.tryEmitNext(ServerSentEvent.builder()
                .event(message.type().toString())
                .data(message.body())
                .id(id)
                .build());

    }
}

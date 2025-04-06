package com.alert.sse;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;

public class SseAlertMessageSender implements AlertMessageSender {
    private final EmitterRepository emitterRepository;

    public SseAlertMessageSender(EmitterRepository emitterRepository) {
        this.emitterRepository = emitterRepository;
    }

    @Override
    public void send(String id, AlertMessage message) {
        emitterRepository.getById(message.targetId())
                .ifPresent(it -> sendEvent(it, id, message));
    }

    private void sendEvent(SseEmitter emitter, String id, AlertMessage alertMessage) {
        SseEmitter.SseEventBuilder eventBuilder = SseEmitter.event()
                .name(alertMessage.type().toString())
                .data(alertMessage.body())
                .id(id);
        try {
            emitter.send(eventBuilder.build());
        } catch (IOException e) {
            emitter.completeWithError(e);
        }
    }
}

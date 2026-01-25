package com.alert.sse;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.sender.AlertMessageSender;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SseAlertMessageSender extends AbstractAlertMessageSender<SseEmitter> {
    public SseAlertMessageSender(TagBasedAlertSessionRepository<SseEmitter> repository) {
        super(repository);
    }

    @Override
    protected void doSend(SseEmitter emitter, String id, AlertMessage message) {
        SseEmitter.SseEventBuilder eventBuilder = SseEmitter.event()
                .name(message.type().toString())
                .data(message.body())
                .id(id);
        try {
            emitter.send(eventBuilder.build());
        } catch (IOException e) {
            emitter.completeWithError(e);
        }
    }
}

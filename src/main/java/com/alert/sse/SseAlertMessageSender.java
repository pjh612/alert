package com.alert.sse;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AbstractAlertMessageSender;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;


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
        } catch (Exception e) {
            emitter.completeWithError(e);
        }
    }
}

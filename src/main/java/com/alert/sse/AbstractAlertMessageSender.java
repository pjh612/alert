package com.alert.sse;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.sender.AlertMessageSender;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractAlertMessageSender<T> implements AlertMessageSender {
    private final TagBasedAlertSessionRepository<T> repository;

    protected AbstractAlertMessageSender(TagBasedAlertSessionRepository<T> repository) {
        this.repository = repository;
    }

    @Override
    public void send(String id, AlertMessage message) {
        Set<T> engines = new HashSet<>();
        for (AlertTarget target : message.targets()) {
            if (target.type() == AlertTarget.TargetType.ID) {
                repository.getById(target.value()).ifPresent(it -> engines.add(it.engine()));
            } else if (target.type() == AlertTarget.TargetType.TAG) {
                repository.getByTag(target.value()).forEach(s -> engines.add(s.engine()));
            }
        }

        engines.forEach(emitter -> doSend(emitter, id, message));
    }


    protected abstract void doSend(T engine, String id, AlertMessage message);
}

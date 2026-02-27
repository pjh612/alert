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
    public void send(String namespace, String id, AlertMessage message) {
        Set<T> engines = new HashSet<>();
        for (AlertTarget target : message.targets()) {
            switch (target.type()) {
                case ID -> repository.getById(namespace, target.value()).ifPresent(it -> engines.add(it.engine()));
                case TAG -> repository.getByTag(namespace, target.value()).forEach(s -> engines.add(s.engine()));
                case BROADCAST -> repository.getAll(namespace).forEach(s -> engines.add(s.engine()));
            }
        }

        engines.forEach(emitter -> doSend(emitter, id, message));
    }


    protected abstract void doSend(T engine, String id, AlertMessage message);
}

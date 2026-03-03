package com.alert.core.messaging.sender;

import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.session.TagBasedAlertSessionRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractAlertMessageSender<T> implements FanoutAlertMessageSender {
    private final TagBasedAlertSessionRepository<T> repository;

    private static final Logger log = LoggerFactory.getLogger(AbstractAlertMessageSender.class);

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

        for (T emitter : engines) {
            try {
                doSend(emitter, id, message);
            } catch (Exception e) {
                log.warn("Failed to send message to engine. messageId={}, reason={}", id, e.getMessage());
            }
        }
    }


    protected abstract void doSend(T engine, String id, AlertMessage message);
}

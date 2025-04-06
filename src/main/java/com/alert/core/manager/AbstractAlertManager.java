package com.alert.core.manager;

import com.alert.core.messaging.bridge.MessagePublisher;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public abstract class AbstractAlertManager<T extends AlertMessage> implements AlertManager {
    protected final MessagePublisher<T> messagePublisher;
    protected final AlertMessageFactory<T> alertMessageFactory;

    private static final Logger log = LoggerFactory.getLogger(AbstractAlertManager.class);


    public AbstractAlertManager(AlertMessageFactory<T> alertMessageFactory, MessagePublisher<T> messagePublisher) {
        this.alertMessageFactory = alertMessageFactory;
        this.messagePublisher = messagePublisher;
    }

    @Override
    public void notice(AlertChannel alertChannel, String targetId, Object message) {
        T alertMessage = alertMessageFactory.onMessage(targetId, message);
        messagePublisher.publish(alertChannel.name(), alertMessage);

        log.debug("alert message published: {}", alertMessage);
    }
}


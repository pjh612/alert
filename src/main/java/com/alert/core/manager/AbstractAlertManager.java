package com.alert.core.manager;

import com.alert.core.messaging.bridge.AlertMessagePublisher;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public abstract class AbstractAlertManager implements AlertManager {
    protected final AlertMessagePublisher alertMessagePublisher;
    protected final AlertMessageFactory alertMessageFactory;

    private static final Logger log = LoggerFactory.getLogger(AbstractAlertManager.class);


    public AbstractAlertManager(AlertMessageFactory alertMessageFactory, AlertMessagePublisher alertMessagePublisher) {
        this.alertMessageFactory = alertMessageFactory;
        this.alertMessagePublisher = alertMessagePublisher;
    }

    @Override
    public void notice(AlertChannel alertChannel, String targetId, Object message) {
        AlertMessage alertMessage = alertMessageFactory.onMessage(targetId, message);
        alertMessagePublisher.publish(alertChannel.name(), alertMessage);

        log.debug("alert message published: {}", alertMessage);
    }
}


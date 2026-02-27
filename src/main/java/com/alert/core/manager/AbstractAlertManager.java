package com.alert.core.manager;

import com.alert.core.messaging.bridge.AlertMessagePublisher;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractAlertManager implements AlertManager {
    protected final AlertMessagePublisher alertMessagePublisher;
    protected final AlertMessageFactory alertMessageFactory;

    private static final Logger log = LoggerFactory.getLogger(AbstractAlertManager.class);

    protected AbstractAlertManager(AlertMessageFactory alertMessageFactory, AlertMessagePublisher alertMessagePublisher) {
        this.alertMessageFactory = alertMessageFactory;
        this.alertMessagePublisher = alertMessagePublisher;
    }

    @Override
    public void notice(AlertChannel channel, String targetId, Object message) {
        notice(channel, List.of(AlertTarget.id(targetId)), message);
    }

    public void noticeByTag(AlertChannel channel, String tag, Object message) {
        notice(channel, List.of(AlertTarget.tag(tag)), message);
    }

    @Override
    public void broadcast(AlertChannel channel, Object message) {
        notice(channel, List.of(AlertTarget.broadcast()), message);
    }

    public void notice(AlertChannel channel, List<AlertTarget> targets, Object message) {
        AlertMessage alertMessage = alertMessageFactory.create(
                channel.namespace(),
                targets,
                DefaultAlertMessageType.MESSAGE,
                message,
                null
        );

        alertMessagePublisher.publish(channel.name(), alertMessage);

        log.debug("Alert message published to targets {}: {}", targets, alertMessage);
    }
}

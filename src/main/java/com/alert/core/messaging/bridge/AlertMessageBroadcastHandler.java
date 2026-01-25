package com.alert.core.messaging.bridge;

import com.alert.core.messaging.broadcaster.MessageBroadcaster;
import com.alert.core.messaging.model.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

public class AlertMessageBroadcastHandler implements TopicAlertMessageHandler {
    private final MessageBroadcaster<String> messageBroadcaster;
    private final ObjectMapper objectMapper;

    private static final Logger log = LoggerFactory.getLogger(AlertMessageBroadcastHandler.class);

    public AlertMessageBroadcastHandler(MessageBroadcaster<String> messageBroadcaster, ObjectMapper objectMapper) {
        this.messageBroadcaster = messageBroadcaster;
        this.objectMapper = objectMapper;
    }

    @Override
    public void handle(String topic, AlertMessage message) {
        try {
            if (log.isTraceEnabled()) {
                log.trace("[AlertMessageBroadcastHandler] handle message {}", message);
            }
            messageBroadcaster.sendMessage(topic, objectMapper.writeValueAsString(message));
        } catch (Exception e) {
            log.error("Failed to send notification: {}", e.getMessage(), e);
        }
    }
}

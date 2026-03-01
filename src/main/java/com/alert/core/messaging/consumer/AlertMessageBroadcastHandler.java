package com.alert.core.messaging.consumer;

import com.alert.core.messaging.broadcaster.MessageBroadcaster;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.BasicAlertMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

public class AlertMessageBroadcastHandler implements TopicAlertMessageHandler {
    private final MessageBroadcaster<String> messageBroadcaster;
    private final ObjectMapper objectMapper;
    private final BasicAlertMessageSender basicSender;

    private static final Logger log = LoggerFactory.getLogger(AlertMessageBroadcastHandler.class);

    public AlertMessageBroadcastHandler(MessageBroadcaster<String> messageBroadcaster, ObjectMapper objectMapper, BasicAlertMessageSender basicSender) {
        this.messageBroadcaster = messageBroadcaster;
        this.objectMapper = objectMapper;
        this.basicSender = basicSender;
    }

    @Override
    public void handle(String topic, AlertMessage message) {
        try {
            if (basicSender != null) {
                basicSender.send(message.namespace(), message.id(), message);
            }
            if (log.isTraceEnabled()) {
                log.trace("[AlertMessageBroadcastHandler] handle message {}", message);
            }
            messageBroadcaster.sendMessage(topic, objectMapper.writeValueAsString(message));
        } catch (Exception e) {
            log.error("Failed to process notification: {}", e.getMessage(), e);
        }
    }
}

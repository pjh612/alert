package com.alert.slack;

import com.alert.core.messaging.broadcaster.MessageConverter;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.sender.AlertMessageSender;
import net.gpedro.integrations.slack.SlackApi;
import net.gpedro.integrations.slack.SlackMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlackAlertMessageSender implements AlertMessageSender {
    private final SlackApi slackApi;
    private final MessageConverter<AlertMessage, String> messageConverter;
    private final String channel;

    private static final Logger log = LoggerFactory.getLogger(SlackAlertMessageSender.class);

    public SlackAlertMessageSender(String webhookUrl, String channel) {
        this(webhookUrl, null, channel);
    }

    public SlackAlertMessageSender(String webhookUrl, MessageConverter<AlertMessage, String> messageConverter, String channel) {
        this.slackApi = new SlackApi(webhookUrl);
        this.messageConverter = messageConverter;
        this.channel = channel;
    }

    @Override
    public void send(String namespace, String id, AlertMessage message) {
        if (!shouldSend(message)) return;

        String content = formatMessage(message);
        SlackMessage slackMessage = new SlackMessage(channel, content);

        try {
            slackApi.call(slackMessage);
        } catch (Exception e) {
            log.warn("Slack message sending failed. messageId = {}, reason = {}", id, e.getMessage(), e);
        }
    }

    private boolean shouldSend(AlertMessage message) {
        return message.type().isCacheable() && !message.isReplay();
    }

    private String formatMessage(AlertMessage message) {
        if (messageConverter == null) {
            return String.valueOf(message.body());
        }
        return messageConverter.convert(message);
    }
}

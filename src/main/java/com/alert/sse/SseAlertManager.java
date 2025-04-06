package com.alert.sse;

import com.alert.cache.AlertCacheManager;
import com.alert.core.manager.AbstractAlertManager;
import com.alert.core.manager.SubscribableAlertManager;
import com.alert.core.messaging.bridge.MessagePublisher;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

public class SseAlertManager<T extends AlertMessage> extends AbstractAlertManager<T> implements SubscribableAlertManager<SseEmitter> {
    private final EmitterRepository emitterRepository;
    private final AlertCacheManager<T> alertCacheManager;
    private final Class<T> messageType;

    private static final Logger log = LoggerFactory.getLogger(SseAlertManager.class);
    private static final String ALERT_KEY_FORMAT = "alert:%s";

    public SseAlertManager(MessagePublisher<T> messagePublisher, AlertMessageFactory<T> alertMessageFactory, EmitterRepository emitterRepository, AlertCacheManager<T> alertCacheManager, Class<T> messageType) {
        super(alertMessageFactory, messagePublisher);
        this.emitterRepository = emitterRepository;
        this.alertCacheManager = alertCacheManager;
        this.messageType = messageType;
    }


    @Override
    public SseEmitter subscribe(AlertChannel alertChannel, String subscriberId, String lastEventId, Long timeoutMillis) {
        SseEmitter emitter = new SseEmitter(timeoutMillis);
        emitterRepository.put(subscriberId, emitter);

        emitter.onTimeout(() -> cleanUpEmitter(subscriberId));
        emitter.onCompletion(() -> cleanUpEmitter(subscriberId));
        emitter.onError(e -> handleEmitterError(subscriberId, e));

        T alertMessage = alertMessageFactory.onConnect(subscriberId);
        messagePublisher.publish(alertChannel.name(), alertMessage);
        if (isReconnected(lastEventId)) {
            republishMissedMessages(alertChannel, subscriberId, lastEventId);
        }

        return emitter;
    }

    private void handleEmitterError(String subscriberId, Throwable e) {
        if (e != null) {
            log.error("Error on SSE emitter id = {}, message = {}", subscriberId, e.getMessage(), e);
        }
        cleanUpEmitter(subscriberId);
    }

    private boolean isReconnected(String lastEventId) {
        return StringUtils.hasText(lastEventId);
    }

    private void republishMissedMessages(AlertChannel alertChannel, String lastEventId, String subscriberId) {
        String key = String.format(ALERT_KEY_FORMAT, subscriberId);
        long offset = parseOffset(lastEventId);

        if (offset >= 0) {
            alertCacheManager.getFromOffset(key, offset, messageType)
                    .stream()
                    .map(it -> alertMessageFactory.onReplayMessage(subscriberId, it))
                    .forEach(it -> messagePublisher.publish(alertChannel.name(), it));
        }
    }

    private long parseOffset(String lastEventId) {
        if (!StringUtils.hasText(lastEventId)) {
            log.warn("LastEventId is empty. Skipping message replay.");
            return -1;
        }
        try {
            return Long.parseLong(lastEventId);
        } catch (NumberFormatException e) {
            log.warn("Invalid lastEventId format: '{}'. Skipping message replay.", lastEventId);
            return -1;
        }
    }

    private void cleanUpEmitter(String subscriberId) {
        SseEmitter emitter = emitterRepository.deleteById(subscriberId);
        if (emitter != null) {
            try {
                emitter.complete();
            } catch (Exception ignored) {
                log.warn("Emitter cleanup failed for id = {}", subscriberId);
            }
        }
    }
}

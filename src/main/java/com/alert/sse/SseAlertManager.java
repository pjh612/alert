package com.alert.sse;

import com.alert.cache.AlertCacheManager;
import com.alert.core.manager.AbstractAlertManager;
import com.alert.core.manager.SubscribableAlertManager;
import com.alert.core.messaging.bridge.AlertMessagePublisher;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.AlertTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SseAlertManager extends AbstractAlertManager implements SubscribableAlertManager<SseEmitter> {
    private final TagBasedAlertSessionRepository<SseEmitter> emitterRepository;
    private final AlertCacheManager alertCacheManager;
    private final AlertMessageSupport support;
    private final Class<? extends AlertMessage> messageType;

    private static final Logger log = LoggerFactory.getLogger(SseAlertManager.class);

    public SseAlertManager(AlertMessagePublisher alertMessagePublisher, AlertMessageFactory alertMessageFactory, TagBasedAlertSessionRepository<SseEmitter> emitterRepository, AlertCacheManager alertCacheManager, AlertMessageSupport support, Class<? extends AlertMessage> messageType) {
        super(alertMessageFactory, alertMessagePublisher);
        this.emitterRepository = emitterRepository;
        this.alertCacheManager = alertCacheManager;
        this.support = support;
        this.messageType = messageType;
    }


    @Override
    public SseEmitter subscribe(AlertChannel alertChannel, String subscriberId, List<String> tags, String lastEventId, Long timeoutMillis) {
        SseEmitter emitter = new SseEmitter(timeoutMillis);
        emitterRepository.put(subscriberId, new HashSet<>(tags), emitter);

        emitter.onTimeout(() -> cleanUpEmitter(subscriberId));
        emitter.onCompletion(() -> cleanUpEmitter(subscriberId));
        emitter.onError(e -> handleEmitterError(subscriberId, e));

        AlertMessage connectMsg = alertMessageFactory.onConnect(subscriberId, null);
        alertMessagePublisher.publish(alertChannel.name(), connectMsg);

        if (isReconnected(lastEventId)) {
            republishMissedMessages(alertChannel, subscriberId, tags, lastEventId);
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

    private void republishMissedMessages(AlertChannel alertChannel, String subscriberId, List<String> tags, String lastEventId) {
        long offset = parseOffset(lastEventId);
        if (offset < 0) return;

        Map<String, AlertMessage> mergedMessages = new TreeMap<>();

        fetchAndMerge(mergedMessages, alertChannel.name(), AlertTarget.id(subscriberId), offset);

        for (String tag : tags) {
            fetchAndMerge(mergedMessages, alertChannel.name(), AlertTarget.tag(tag), offset);
        }

        mergedMessages.values().forEach(msg ->
                alertMessagePublisher.publish(alertChannel.name(), alertMessageFactory.onReplay(subscriberId, msg, null))
        );
    }

    private void fetchAndMerge(Map<String, AlertMessage> map, String topic, AlertTarget target, long offset) {
        String key = support.resolveCacheKey(topic, target);
        alertCacheManager.getFromOffset(key, offset, messageType)
                .forEach(msg -> map.putIfAbsent(msg.id(), msg));
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
        AlertSession<SseEmitter> alertSession = emitterRepository.deleteById(subscriberId);
        SseEmitter emitter = alertSession.engine();
        if (emitter != null) {
            try {
                emitter.complete();
            } catch (Exception ignored) {
                log.warn("Emitter cleanup failed for id = {}", subscriberId);
            }
        }
    }
}

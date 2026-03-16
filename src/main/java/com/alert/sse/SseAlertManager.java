package com.alert.sse;

import com.alert.core.cache.AlertCacheManager;
import com.alert.core.manager.AbstractAlertManager;
import com.alert.core.manager.SubscribableAlertManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.publisher.AlertMessagePublisher;
import com.alert.core.session.AlertSession;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;

public class SseAlertManager extends AbstractAlertManager implements SubscribableAlertManager<SseEmitter> {
    private final TagBasedAlertSessionRepository<SseEmitter> emitterRepository;
    private final AlertCacheManager alertCacheManager;
    private final AlertMessageSupport support;
    private final Class<? extends AlertMessage> messageType;
    private final Executor dispatchExecutor;

    private static final Logger log = LoggerFactory.getLogger(SseAlertManager.class);

    public SseAlertManager(AlertMessagePublisher alertMessagePublisher, AlertMessageFactory alertMessageFactory, TagBasedAlertSessionRepository<SseEmitter> emitterRepository, AlertCacheManager alertCacheManager, AlertMessageSupport support, Class<? extends AlertMessage> messageType, Executor dispatchExecutor) {
        super(alertMessageFactory, alertMessagePublisher);
        this.emitterRepository = emitterRepository;
        this.alertCacheManager = alertCacheManager;
        this.support = support;
        this.messageType = messageType;
        this.dispatchExecutor = dispatchExecutor;
    }

    @Override
    public SseEmitter subscribe(AlertChannel alertChannel, String subscriberId, List<String> tags, String lastEventId, Long timeoutMillis) {
        String namespace = alertChannel.namespace();
        SseEmitter emitter = new SseEmitter(timeoutMillis);
        List<String> safeTags = tags != null ? tags : List.of();

        AlertSession<SseEmitter> old = emitterRepository.put(namespace, subscriberId, new HashSet<>(safeTags), emitter);
        if (old != null) {
            try {
                old.engine().complete();
            } catch (Exception ignored) {
            }
        }

        emitter.onTimeout(() -> cleanUpEmitter(namespace, subscriberId, emitter));
        emitter.onCompletion(() -> cleanUpEmitter(namespace, subscriberId, emitter));
        emitter.onError(e -> handleEmitterError(namespace, subscriberId, emitter, e));

        AlertMessage connectMsg = alertMessageFactory.onConnect(namespace, subscriberId, null);
        dispatchExecutor.execute(() -> alertMessagePublisher.publish(alertChannel.name(), connectMsg));

        if (isReconnected(lastEventId)) {
            dispatchExecutor.execute(() -> replayMissedMessages(emitter, alertChannel.namespace(), subscriberId, safeTags, lastEventId));
        }

        return emitter;
    }

    private void handleEmitterError(String namespace, String subscriberId, SseEmitter emitter, Throwable e) {
        if (e != null) {
            log.error("Error on SSE emitter id = {}, message = {}", subscriberId, e.getMessage(), e);
        }
        cleanUpEmitter(namespace, subscriberId, emitter);
    }

    private boolean isReconnected(String lastEventId) {
        return StringUtils.hasText(lastEventId);
    }

    private void replayMissedMessages(SseEmitter emitter, String namespace, String subscriberId, List<String> tags, String lastEventId) {
        Map<String, AlertMessage> mergedMessages = new TreeMap<>();

        fetchAndMerge(mergedMessages, namespace, AlertTarget.id(subscriberId), lastEventId);
        for (String tag : tags) {
            fetchAndMerge(mergedMessages, namespace, AlertTarget.tag(tag), lastEventId);
        }
        fetchAndMerge(mergedMessages, namespace, AlertTarget.broadcast(), lastEventId);

        mergedMessages.values().forEach(msg -> {
            try {
                emitter.send(SseEmitter.event()
                        .id(msg.id())
                        .name(msg.type().toString())
                        .data(msg.body()));
            } catch (Exception e) {
                log.warn("Failed to replay message id={} to subscriber={}", msg.id(), subscriberId, e);
            }
        });
    }

    private void fetchAndMerge(Map<String, AlertMessage> map, String namespace, AlertTarget target, String offset) {
        String key = support.resolveCacheKey(namespace, target);
        alertCacheManager.getFromOffset(key, offset, messageType)
                .forEach(msg -> map.putIfAbsent(msg.id(), msg));
    }

    private void cleanUpEmitter(String namespace, String subscriberId, SseEmitter emitter) {
        if(log.isTraceEnabled()) {
            log.trace("Cleaning up emitter for subscriberId={}, namespace={}", subscriberId, namespace);
        }
        emitterRepository.getById(namespace, subscriberId)
                .filter(session -> session.engine() == emitter)
                .ifPresent(session -> emitterRepository.deleteById(namespace, subscriberId));
    }
}

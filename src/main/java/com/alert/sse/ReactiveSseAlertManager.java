package com.alert.sse;

import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.manager.ReactiveAbstractAlertManager;
import com.alert.core.manager.ReactiveSubscribableAlertManager;
import com.alert.core.messaging.publisher.ReactiveAlertMessagePublisher;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ReactiveSseAlertManager extends ReactiveAbstractAlertManager implements ReactiveSubscribableAlertManager<Flux<ServerSentEvent<Object>>> {
    private final TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> repository;
    private final ReactiveAlertCacheManager cacheManager;
    private final AlertMessageSupport support;
    private final Class<? extends AlertMessage> messageType;

    private static final Logger log = LoggerFactory.getLogger(ReactiveSseAlertManager.class);

    public ReactiveSseAlertManager(ReactiveAlertMessagePublisher alertMessagePublisher, AlertMessageFactory alertMessageFactory, TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> repository, ReactiveAlertCacheManager cacheManager, AlertMessageSupport support, Class<? extends AlertMessage> messageType) {
        super(alertMessageFactory, alertMessagePublisher);
        this.repository = repository;
        this.cacheManager = cacheManager;
        this.support = support;
        this.messageType = messageType;
    }

    @Override
    public Flux<ServerSentEvent<Object>> subscribe(AlertChannel channel, String subscriberId, List<String> tags, String lastEventId, Long timeoutMillis) {
        String namespace = channel.namespace();
        Sinks.Many<ServerSentEvent<Object>> sink = Sinks.many().multicast().onBackpressureBuffer();
        repository.put(namespace, subscriberId, new HashSet<>(tags), sink);

        Mono.fromRunnable(() -> alertMessagePublisher.publish(channel.name(), alertMessageFactory.onConnect(namespace, subscriberId, null)))
                .subscribe();

        Flux<ServerSentEvent<Object>> recoveryFlux = Flux.empty();
        if (StringUtils.hasText(lastEventId)) {
            long offset = parseOffset(lastEventId);
            if (offset >= 0) {
                recoveryFlux = getRecoveryFlux(namespace, subscriberId, tags, offset);
            }
        }

        return Flux.concat(recoveryFlux, sink.asFlux())
                .timeout(Duration.ofMillis(timeoutMillis))
                .doFinally(it -> repository.deleteById(namespace, subscriberId));
    }

    private long parseOffset(String lastEventId) {
        try {
            return Long.parseLong(lastEventId);
        } catch (NumberFormatException e) {
            log.warn("Invalid lastEventId format: '{}'. Skipping replay.", lastEventId);
            return -1;
        }
    }

    private Flux<ServerSentEvent<Object>> getRecoveryFlux(String namespace, String id, List<String> tags, long offset) {
        List<Flux<? extends AlertMessage>> sources = new ArrayList<>();
        sources.add(cacheManager.getFromOffset(support.resolveCacheKey(namespace, AlertTarget.id(id)), offset, messageType));
        tags.forEach(tag -> sources.add(cacheManager.getFromOffset(support.resolveCacheKey(namespace, AlertTarget.tag(tag)), offset, messageType)));
        sources.add(cacheManager.getFromOffset(support.resolveCacheKey(namespace, AlertTarget.broadcast()), offset, messageType));

        return Flux.merge(sources)
                .distinct(AlertMessage::id)
                .sort(Comparator.comparing(AlertMessage::id))
                .map(msg -> ServerSentEvent.builder().id(msg.id()).event(msg.type().toString()).data(msg.body()).build());
    }
}

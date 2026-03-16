package com.alert.sse;

import com.alert.core.cache.ReactiveAlertCacheManager;
import com.alert.core.manager.ReactiveAbstractAlertManager;
import com.alert.core.manager.ReactiveSubscribableAlertManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.model.AlertMessageFactory;
import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.publisher.ReactiveAlertMessagePublisher;
import com.alert.core.session.AlertSession;
import com.alert.core.session.TagBasedAlertSessionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ReactiveSseAlertManager extends ReactiveAbstractAlertManager implements ReactiveSubscribableAlertManager<Flux<ServerSentEvent<Object>>> {
    private final TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> repository;
    private final ReactiveAlertCacheManager cacheManager;
    private final AlertMessageSupport support;
    private final Class<? extends AlertMessage> messageType;
    private final Comparator<String> idComparator;
    private static final Logger log = LoggerFactory.getLogger(ReactiveSseAlertManager.class);

    public ReactiveSseAlertManager(ReactiveAlertMessagePublisher alertMessagePublisher, AlertMessageFactory alertMessageFactory, TagBasedAlertSessionRepository<Sinks.Many<ServerSentEvent<Object>>> repository, ReactiveAlertCacheManager cacheManager, AlertMessageSupport support, Class<? extends AlertMessage> messageType, Comparator<String> idComparator) {
        super(alertMessageFactory, alertMessagePublisher);
        this.repository = repository;
        this.cacheManager = cacheManager;
        this.support = support;
        this.messageType = messageType;
        this.idComparator = idComparator;
    }

    @Override
    public Flux<ServerSentEvent<Object>> subscribe(AlertChannel channel, String subscriberId, List<String> tags, String lastEventId, Long timeoutMillis) {
        String namespace = channel.namespace();
        List<String> safeTags = tags != null ? tags : List.of();
        AlertSession<Sinks.Many<ServerSentEvent<Object>>> deletedSession = repository.deleteById(namespace, subscriberId);
        if(deletedSession != null) {
            deletedSession.engine().tryEmitComplete();
        }

        Sinks.Many<ServerSentEvent<Object>> sink = Sinks.many().multicast().onBackpressureBuffer();
        repository.put(namespace, subscriberId, new HashSet<>(safeTags), sink);

        AlertMessage connectMsg = alertMessageFactory.onConnect(namespace, subscriberId, null);
        Flux<ServerSentEvent<Object>> connectFlux = Flux.just(
                ServerSentEvent.builder()
                        .id(connectMsg.id())
                        .event(connectMsg.type().toString())
                        .data(connectMsg.body())
                        .build()
        );

        boolean hasOffset = StringUtils.hasText(lastEventId);

        AtomicReference<String> maxRecoveryId = new AtomicReference<>(lastEventId);

        Flux<ServerSentEvent<Object>> recoveryFlux = Flux.empty();
        if (hasOffset) {
            recoveryFlux = getRecoveryFlux(namespace, subscriberId, safeTags, lastEventId)
                    .doOnNext(e -> {
                        String eid = e.id();
                        if (eid != null) {
                            maxRecoveryId.accumulateAndGet(eid, (current, next) ->
                                    (current == null || idComparator.compare(current, next) < 0) ? next : current
                            );
                        }
                    });
        }

        Flux<ServerSentEvent<Object>> liveFlux = hasOffset
                ? sink.asFlux().filter(e -> isAfterRecovery(e.id(), maxRecoveryId))
                : sink.asFlux();

        return Flux.concat(connectFlux, recoveryFlux, liveFlux)
                .timeout(Duration.ofMillis(timeoutMillis), Flux.empty())
                .doFinally(it -> repository.deleteById(namespace, subscriberId));
    }

    private boolean isAfterRecovery(String eventId, AtomicReference<String> maxRecoveryIdRef) {
        if (eventId == null) return false;
        String maxId = maxRecoveryIdRef.get();
        if (maxId == null) return false;

        return idComparator.compare(eventId, maxId) > 0;
    }

    private Flux<ServerSentEvent<Object>> getRecoveryFlux(String namespace, String id, List<String> tags, String offset) {
        List<Flux<? extends AlertMessage>> sources = new ArrayList<>();
        sources.add(cacheManager.getFromOffset(support.resolveCacheKey(namespace, AlertTarget.id(id)), offset, messageType));
        tags.forEach(tag -> sources.add(cacheManager.getFromOffset(support.resolveCacheKey(namespace, AlertTarget.tag(tag)), offset, messageType)));
        sources.add(cacheManager.getFromOffset(support.resolveCacheKey(namespace, AlertTarget.broadcast()), offset, messageType));

        return Flux.merge(sources)
                .filter(m -> m.id() != null)
                .distinct(AlertMessage::id)
                .sort((m1, m2) -> idComparator.compare(m1.id(), m2.id()))
                .map(msg -> ServerSentEvent.builder().id(msg.id()).event(msg.type().toString()).data(msg.body()).build());
    }
}

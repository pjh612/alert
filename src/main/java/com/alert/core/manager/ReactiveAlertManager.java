package com.alert.core.manager;

import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertTarget;
import reactor.core.publisher.Mono;

import java.util.List;

public interface ReactiveAlertManager {
    Mono<Void> notice(AlertChannel alertChannel, String targetId, Object message);

    Mono<Void> noticeByTag(AlertChannel alertChannel, String tag, Object message);

    Mono<Void> notice(AlertChannel alertChannel, List<AlertTarget> targets, Object message);
}

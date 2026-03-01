package com.alert.infra.kafka;

import com.alert.core.cache.AlertCacheManager;
import com.alert.core.messaging.broadcaster.AlertMessageSupport;
import com.alert.core.messaging.model.AlertMessage;
import com.alert.core.messaging.publisher.AlertMessagePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaMessagePublisher implements AlertMessagePublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaMessagePublisher.class);

    private final AlertCacheManager alertCacheManager;
    private final AlertMessageSupport support;
    private final KafkaTemplate<String, AlertMessage> kafkaTemplate;
    private final PartitionKeyStrategy partitionKeyStrategy;

    public KafkaMessagePublisher(AlertCacheManager alertCacheManager, AlertMessageSupport support, KafkaTemplate<String, AlertMessage> kafkaTemplate, PartitionKeyStrategy partitionKeyStrategy) {
        this.alertCacheManager = alertCacheManager;
        this.support = support;
        this.kafkaTemplate = kafkaTemplate;
        this.partitionKeyStrategy = partitionKeyStrategy;
    }

    @Override
    public void publish(String channel, AlertMessage message) {
        if(message.id() == null) {
            throw new IllegalArgumentException("Message id cannot be null");
        }

        if (message.type().isCacheable()) {
            message.targets().forEach(target -> {
                String cacheKey = support.resolveCacheKey(message.namespace(), target);
                alertCacheManager.save(cacheKey, message.id(), message);
            });
        }

        String partitionKey = partitionKeyStrategy.resolve(message);
        kafkaTemplate.send(channel, partitionKey, message);

        log.debug("channel:{}, partitionKey:{}, alertMessage:{}", channel, partitionKey, message);
    }
}

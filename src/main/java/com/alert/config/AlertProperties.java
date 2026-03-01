package com.alert.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;

@ConfigurationProperties(prefix = "alert")
public record AlertProperties(List<String> topics, String groupId, Integer concurrency, Long cacheTtlSeconds, Long nodeId) {
    public AlertProperties {
        topics = (topics != null) ? topics : List.of("default-topic");
        groupId = (groupId != null) ? groupId : "default-group";
        concurrency = (concurrency != null) ? concurrency : 128;
        cacheTtlSeconds = (cacheTtlSeconds != null) ? cacheTtlSeconds : 1800L;
        nodeId = (nodeId != null) ? nodeId : 0L;
    }
}

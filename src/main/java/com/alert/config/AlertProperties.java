package com.alert.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "alert")
public record AlertProperties(List<String> topics, String groupId) {
    public AlertProperties {
        topics = (topics != null) ? topics : List.of("default-topic");
        groupId = (groupId != null) ? groupId : "default-group";
    }
}
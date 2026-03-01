package com.alert.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "alert.kafka")
public record AlertKafkaProperties(DlqProperties dlq) {

    public AlertKafkaProperties {
        dlq = (dlq != null) ? dlq : new DlqProperties(null, null);
    }

    public record DlqProperties(String topicSuffix, BackoffProperties backoff) {
        public DlqProperties {
            topicSuffix = (topicSuffix != null) ? topicSuffix : ".dlq";
            backoff = (backoff != null) ? backoff : new BackoffProperties(null, null);
        }
    }

    public record BackoffProperties(Long interval, Long maxAttempts) {
        public BackoffProperties {
            interval = (interval != null) ? interval : 1000L;
            maxAttempts = (maxAttempts != null) ? maxAttempts : 3L;
        }
    }
}

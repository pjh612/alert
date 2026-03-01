package com.alert.core.messaging.id;

public class TimestampIdGenerator implements IdGenerator<String> {
    @Override
    public String nextId() {
        return Long.toString(System.currentTimeMillis());
    }
}

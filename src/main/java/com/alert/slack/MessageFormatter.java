package com.alert.slack;

public interface MessageFormatter<T> {
    String format(T message);
}

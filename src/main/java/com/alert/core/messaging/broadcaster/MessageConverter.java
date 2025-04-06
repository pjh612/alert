package com.alert.core.messaging.broadcaster;

@FunctionalInterface
public interface MessageConverter<I,O> {
    O convert(I object);
}

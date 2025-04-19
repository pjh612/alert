package com.alert.core.messaging.broadcaster;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class StringJsonMessageConverter<O> implements MessageConverter<String, O> {
    private final ObjectMapper objectMapper;
    private final Class<O> targetType;


    public StringJsonMessageConverter(ObjectMapper objectMapper, Class<O> targetType) {
        this.objectMapper = objectMapper;
        this.targetType = targetType;
    }

    @Override
    public O convert(String input) {
        try {
            return objectMapper.readValue(input, targetType);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }
}

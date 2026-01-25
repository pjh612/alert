package com.alert.core.messaging.broadcaster;


import tools.jackson.databind.ObjectMapper;

public class StringJsonMessageConverter<O> implements MessageConverter<String, O> {
    private final ObjectMapper objectMapper;
    private final Class<O> targetType;


    public StringJsonMessageConverter(ObjectMapper objectMapper, Class<O> targetType) {
        this.objectMapper = objectMapper;
        this.targetType = targetType;
    }

    @Override
    public O convert(String input) {
        return objectMapper.readValue(input, targetType);
    }
}

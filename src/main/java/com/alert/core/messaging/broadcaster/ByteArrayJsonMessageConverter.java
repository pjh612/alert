package com.alert.core.messaging.broadcaster;

import tools.jackson.databind.ObjectMapper;

public class ByteArrayJsonMessageConverter<O> implements MessageConverter<byte[], O> {
    private final ObjectMapper objectMapper;
    private final Class<O> targetType;


    public ByteArrayJsonMessageConverter(ObjectMapper objectMapper, Class<O> targetType) {
        this.objectMapper = objectMapper;
        this.targetType = targetType;
    }

    @Override
    public O convert(byte[] input) {
        return objectMapper.readValue(input, targetType);
    }
}

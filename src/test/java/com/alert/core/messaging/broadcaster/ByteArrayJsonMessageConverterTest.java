package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.DefaultAlertMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ByteArrayJsonMessageConverterTest {

    ObjectMapper objectMapper;
    ByteArrayJsonMessageConverter<DefaultAlertMessage> converter;

    @BeforeEach
    void setUp() {
        objectMapper = JsonMapper.builder().build();
        converter = new ByteArrayJsonMessageConverter<>(objectMapper, DefaultAlertMessage.class);
    }

    @Test
    @DisplayName("JSON 바이트 배열을 객체로 변환한다")
    void convert_byteArrayToObject() {
        DefaultAlertMessage original = TestAlertMessageFactory.create();
        byte[] json = objectMapper.writeValueAsBytes(original);

        DefaultAlertMessage result = converter.convert(json);

        assertThat(result.id()).isEqualTo(original.id());
        assertThat(result.namespace()).isEqualTo(original.namespace());
        assertThat(result.body()).isEqualTo(original.body());
    }
}

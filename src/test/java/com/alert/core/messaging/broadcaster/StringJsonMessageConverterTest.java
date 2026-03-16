package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.DefaultAlertMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class StringJsonMessageConverterTest {

    ObjectMapper objectMapper;
    StringJsonMessageConverter<DefaultAlertMessage> converter;

    @BeforeEach
    void setUp() {
        objectMapper = JsonMapper.builder().build();
        converter = new StringJsonMessageConverter<>(objectMapper, DefaultAlertMessage.class);
    }

    @Test
    @DisplayName("JSON 문자열을 객체로 변환한다")
    void convert_jsonStringToObject() {
        DefaultAlertMessage original = TestAlertMessageFactory.create();
        String json = objectMapper.writeValueAsString(original);

        DefaultAlertMessage result = converter.convert(json);

        assertThat(result.id()).isEqualTo(original.id());
        assertThat(result.namespace()).isEqualTo(original.namespace());
        assertThat(result.body()).isEqualTo(original.body());
    }
}

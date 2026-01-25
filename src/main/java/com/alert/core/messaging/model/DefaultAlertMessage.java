package com.alert.core.messaging.model;

import tools.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Map;

public record DefaultAlertMessage(String id, List<AlertTarget> targets,
                                  @JsonDeserialize(as = DefaultAlertMessageType.class)
                                  AlertMessageType type,
                                  Object body,
                                  boolean isReplay,
                                  Map<String, String> attributes) implements AlertMessage {
}

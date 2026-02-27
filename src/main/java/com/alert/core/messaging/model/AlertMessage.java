package com.alert.core.messaging.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface AlertMessage {
    String id();

    String namespace();

    AlertMessageType type();

    List<AlertTarget> targets();

    Object body();

    boolean isReplay();

    default Map<String, String> attributes() {
        return Collections.emptyMap();
    }
}

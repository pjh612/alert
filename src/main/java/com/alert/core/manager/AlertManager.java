package com.alert.core.manager;

import com.alert.core.messaging.model.AlertChannel;

public interface AlertManager {
    void notice(AlertChannel alertChannel, String targetId, Object message);
}

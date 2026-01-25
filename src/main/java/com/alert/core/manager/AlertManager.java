package com.alert.core.manager;

import com.alert.core.messaging.model.AlertChannel;
import com.alert.core.messaging.model.AlertTarget;

import java.util.List;

public interface AlertManager {
    void notice(AlertChannel alertChannel, String targetId, Object message);

    void noticeByTag(AlertChannel alertChannel, String tag, Object message);

    void notice(AlertChannel alertChannel, List<AlertTarget> targets, Object message);
}

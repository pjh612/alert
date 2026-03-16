package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.model.AlertTarget;
import com.alert.core.messaging.model.DefaultAlertMessage;
import com.alert.core.messaging.model.DefaultAlertMessageType;

import java.util.List;

final class TestAlertMessageFactory {

    private TestAlertMessageFactory() {
    }

    static DefaultAlertMessage create() {
        return new DefaultAlertMessage(
                "1700000000001",
                "test-ns",
                List.of(AlertTarget.id("user1")),
                DefaultAlertMessageType.MESSAGE,
                "test-body",
                false,
                null
        );
    }
}

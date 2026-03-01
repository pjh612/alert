package com.alert.core.messaging.sender;

import com.alert.core.messaging.model.AlertMessage;

import java.util.List;

/**
 * 여러 {@link FanoutAlertMessageSender}를 하나로 조합하는 Composite 구현체.
 *
 * <p>등록된 모든 Sender에 동일한 메시지를 순서대로 위임한다.
 */
public class FanoutAlertMessageDelegateSender implements FanoutAlertMessageSender {
    private final List<FanoutAlertMessageSender> senders;

    public FanoutAlertMessageDelegateSender(List<FanoutAlertMessageSender> senders) {
        this.senders = senders;
    }

    @Override
    public void send(String namespace, String id, AlertMessage message) {
        senders.forEach(s -> s.send(namespace, id, message));
    }
}

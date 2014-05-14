package com.netflix.suro.event;

import java.util.List;

import com.netflix.suro.message.Message;

public class PartitionEvent {
    private final List<Message> msgList;

    public PartitionEvent(List<Message> msgList) {
        this.msgList = msgList;
    }

    public List<Message> getMsgList() {
        return msgList;
    }
}

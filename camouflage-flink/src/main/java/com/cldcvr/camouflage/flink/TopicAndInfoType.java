package com.cldcvr.camouflage.flink;

import com.cldcvr.camouflage.core.info.types.AbstractInfoType;

import java.util.Map;
import java.util.Set;

final class TopicAndInfoType {
    private final String topic;
    private final Map<String, Set<AbstractInfoType>> columnTypes;

    public TopicAndInfoType(String topic, Map<String, Set<AbstractInfoType>> columnTypes) {
        this.topic = topic;
        this.columnTypes = columnTypes;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, Set<AbstractInfoType>> getColumnTypes() {
        return columnTypes;
    }
}

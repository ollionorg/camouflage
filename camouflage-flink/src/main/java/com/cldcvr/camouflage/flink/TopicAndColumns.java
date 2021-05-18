package com.cldcvr.camouflage.flink;

import com.cldcvr.camouflage.core.json.serde.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Holds Topics and columns for DLP serde
 */
public class TopicAndColumns {
    private final String topic;
    private final List<ColumnMetadata> columnMetadataList;

    public TopicAndColumns(@JsonProperty("topic") String topic, @JsonProperty("columns") List<ColumnMetadata> columnMetadataList) {
        this.topic = topic;
        this.columnMetadataList = columnMetadataList;
    }

    public String getTopic() {
        return topic;
    }

    public List<ColumnMetadata> getColumnMetadataList() {
        return columnMetadataList;
    }

    @Override
    public String toString() {
        return "TopicAndColumns{" +
                "topic='" + topic + '\'' +
                ", columnMetadataList=" + columnMetadataList +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicAndColumns that = (TopicAndColumns) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(columnMetadataList, that.columnMetadataList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, columnMetadataList);
    }
}

package com.cldcvr.camouflage.flink;


import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.util.MapToInfoType;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MaskFlinkRecord extends RichFlatMapFunction<JsonNode, JsonNode> {
    private final Logger LOG = LoggerFactory.getLogger(MaskFlinkRecord.class);

    private FlinkCamouflageSerDe serDe;
    Map<String, Map<String, Set<AbstractInfoType>>> topicAndColumnInfoTypes;

    @Override
    public void open(Configuration parameters) throws Exception {
        String dlpSerDeString = parameters.getString("", null);
        serDe = new ObjectMapper().readValue(dlpSerDeString, FlinkCamouflageSerDe.class);
        topicAndColumnInfoTypes = serDe.getList().stream()
                .map(topicAndColumns -> {
                    Map<String, Set<AbstractInfoType>> columnAndAbstractType = new HashMap<>();
                    topicAndColumns.getColumnMetadataList().forEach(r -> {
                        try {
                            columnAndAbstractType.put(r.getColumn(), MapToInfoType.toInfoTypeMapping(r.getDlpTypes()));
                        } catch (CamouflageApiException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    return new TopicAndInfoType(topicAndColumns.getTopic(), columnAndAbstractType);
                }).collect(Collectors.toMap(TopicAndInfoType::getTopic, TopicAndInfoType::getColumnTypes));
    }

    @Override
    public void flatMap(JsonNode value, Collector<JsonNode> collector) throws Exception {
        try {
            String topic = value.get("metadata").get("topic").asText();
            ObjectNode node = (ObjectNode) value;
            Map<String, Set<AbstractInfoType>> colMap = topicAndColumnInfoTypes.get(topic);
            if (colMap != null) {
                colMap.forEach((k, v) -> {
                    if (k == null || v == null)
                        return;
                    String item = node.get(k).asText();
                    String data = item == null ? null : item;
                    if (data != null) {
                        Iterator<AbstractInfoType> it = v.iterator();
                        while (it.hasNext()) {
                            AbstractInfoType infoType = it.next();
                            data = infoType.getMaskStrategy().applyMaskStrategy(data, infoType.regex());
                            break;
                        }
                        node.put(k, data);
                    }
                });
            }
            collector.collect(node);
        } catch (Exception e) {
            LOG.error("Error while masking record {}", value, e);
        }

    }

    @Override
    public void close() throws Exception {
        serDe = null;
        //GC the serde
    }


}

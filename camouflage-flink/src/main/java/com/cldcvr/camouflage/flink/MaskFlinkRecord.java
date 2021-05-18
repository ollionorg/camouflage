package com.cldcvr.camouflage.flink;


import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.util.MapToInfoType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MaskFlinkRecord extends RichFlatMapFunction<ObjectNode, ObjectNode> {
    private final Logger LOG = LoggerFactory.getLogger(MaskFlinkRecord.class);
    public static final String DLP_METADATA = "dlpMetadata";
    private FlinkCamouflageSerDe serDe;
    Map<String, Map<String, Set<AbstractInfoType>>> topicAndColumnInfoTypes;

    /**
     * Flat map open method constructs the FlinkCamouflageSerDe and also generates the map of
     * Topic to map of Column to which infotypes will be applied.
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool jobParameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String dlpSerDeString = jobParameters.get(DLP_METADATA, null);
        LOG.info("Flink Dlp json is " + dlpSerDeString);
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

    /**
     * Per record processor of DLP
     * Takes a json node and then applies DLP
     * Each json node has metadata field in which the topic fields tells us which topic the record belongs to.
     * Based on the topic we look up in the map and apply the correct masking types.
     * If record does not belong to a topic it is sent downstream as is.
     * If record throws an exception during DLP or otherwise, the record is logged and discarded by flink
     *
     * @param value     input record to apply DLP to
     * @param collector
     * @throws Exception
     */

    @Override
    public void flatMap(ObjectNode value, Collector<ObjectNode> collector) throws Exception {
        try {
            String topic = value.get("metadata").get("topic").asText();
            ObjectNode node = value;
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

    /**
     * Life cycle clears the serde to make it applicable for GC
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        serDe = null;
        //GC the serde
    }

    /**
     * Supporting POJO holds topic as key and Map of Column names to Abstract info types set
     */
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

}

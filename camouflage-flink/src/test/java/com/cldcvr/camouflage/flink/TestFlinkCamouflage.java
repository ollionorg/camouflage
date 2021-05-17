package com.cldcvr.camouflage.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TestFlinkCamouflage extends TestBase {


    @Test
    public void testFlinkCamouflageWithOneTopic() throws Exception {
        String json = "{\"DLPMetaData\":[{\"topic\":\"mysql-test-1234-flink-test_2.rdstest.test_500gb\",\n" +
                "\t\"columns\":[{\"column\":\"phone\",\n" +
                "\t\t\"dlpTypes\":[{\"info_type\":\"PHONE_NUMBER\",\"mask_type\":\"REPLACE_CONFIG\",\n" +
                "\t\t\"replace\":\"*\",\"salt\":\"\"}]}]}]}";

        List<JsonNode> jsonNodes = generateTestRecords("mysql-test-1234-flink-test_2.rdstest.test_500gb", 10);
        Iterator<JsonNode> iterator = process(json, jsonNodes);
        match(iterator, "phone", "********", jsonNodes.size());
    }

    @Test
    public void testFlinkCamouflageMoreThanOneTopic() throws Exception {
        String TOPIC_1 = "mysql-test-1234-flink-test_2.rdstest.test_500gb";
        String TOPIC_2 = "mysql-test-1234-flink-test_3.rdstest.test_500gb";
        String TOPIC_3 = "mysql-test-1234-flink-test_4.rdstest.test_500gb";
        String json = "{\n" +
                "\"DLPMetaData\":[{\n" +
                "\t\"topic\":\"" + TOPIC_1 + "\",\n" +
                "\t\"columns\":[{\n" +
                "\t\t\"column\":\"phone\",\n" +
                "\t\t\"dlpTypes\":[{\n" +
                "\t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                "\t\t\t\"mask_type\":\"REDACT_CONFIG\",\n" +
                "\t\t\t\"replace\":\"*\"\n" +
                "\t\t}]\n" +
                "\t}]\n" +
                "},{\n" +
                "\t\"topic\":\"" + TOPIC_2 + "\",\n" +
                "\t\"columns\":[{\n" +
                "\t\t\"column\":\"name\",\n" +
                "\t\t\"dlpTypes\":[{\n" +
                "\t\t\t\"info_type\":\"GENERIC\",\n" +
                "\t\t\t\"mask_type\":\"REDACT_CONFIG\",\n" +
                "\t\t\t\"replace\":\"*\"\n" +
                "\t\t}]\n" +
                "\t}]\n" +
                "}\n" +
                "]\n" +
                "}";

        List<JsonNode> topic1Records = generateTestRecords(TOPIC_1, 10);
        List<JsonNode> topic2Records = generateTestRecords(TOPIC_2, 10);
        List<JsonNode> topic3Records = generateTestRecords(TOPIC_3, 10);
        List<JsonNode> data = new ArrayList<>();
        data.addAll(topic3Records);
        data.addAll(topic2Records);
        data.addAll(topic1Records);
        Iterator<JsonNode> jsonNodeIterator = process(json, data);
        //since flink's iterator can only be iterated over once, we need to cache records
        List<JsonNode> recordCache = cache(jsonNodeIterator);
        List<JsonNode> untouchedNodes = recordCache.stream().filter(j -> filterByTopic(j, TOPIC_3)).collect(Collectors.toList());
        //Assert that phone is redacted
        topicAwareMatch(TOPIC_1, recordCache.iterator(), "phone", "********");
        //Assert that name is redacted
        topicAwareMatch(TOPIC_2, recordCache.iterator(), "name", "*****");
        //Assert it topic 3 remains untouched as there is no dlp for topic 3
        Iterator<JsonNode> iterator = untouchedNodes.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            JsonNode next = iterator.next();
            Assert.assertTrue(topic3Records.contains(next));
            count++;
        }
        Assert.assertEquals(topic3Records.size(), count);
    }

    private List<JsonNode> cache(Iterator<JsonNode> jsonNodeIterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(jsonNodeIterator, 0), false)
                .collect(Collectors.toList());
    }

    private Iterator<JsonNode> process(String json, List<JsonNode> data) throws java.io.IOException {
        setCamouflageJsonToEnv(new String[]{"--" + MaskFlinkRecord.DLP_METADATA, json});
        SingleOutputStreamOperator<JsonNode> jsonStream = env.fromCollection(data)
                .flatMap(new MaskFlinkRecord());
        return DataStreamUtils.collect(jsonStream);
    }


    @Test
    public void testWithMultipleColumnsInDlp() throws IOException {
        String TOPIC_1 = "mysql-test-1234-flink-test_2.rdstest.test_500gb";
        String json = "{\n" +
                "\"DLPMetaData\":[{\n" +
                "\t\"topic\":\"" + TOPIC_1 + "\",\n" +
                "\t\"columns\":[{\n" +
                "\t\t\"column\":\"phone\",\n" +
                "\t\t\"dlpTypes\":[{\n" +
                "\t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                "\t\t\t\"mask_type\":\"REDACT_CONFIG\",\n" +
                "\t\t\t\"replace\":\"*\"\n" +
                "\t\t}]\n" +
                "\t},{\n" +
                "\t\t\"column\":\"name\",\n" +
                "\t\t\"dlpTypes\":[{\n" +
                "\t\t\t\"info_type\":\"GENERIC\",\n" +
                "\t\t\t\"mask_type\":\"REDACT_CONFIG\",\n" +
                "\t\t\t\"replace\":\"*\"\n" +
                "\t\t}]\n" +
                "\t}]\n" +
                "}\n" +
                "]\n" +
                "}";

        List<JsonNode> topic1Records = generateTestRecords(TOPIC_1, 10);
        Iterator<JsonNode> jsonData = process(json, topic1Records);
        List<JsonNode> cache = cache(jsonData);
        match(cache.iterator(), "phone", "********", cache.size());
        match(cache.iterator(), "name", "*****", cache.size());
    }

    public void match(Iterator<JsonNode> iterator, String key, String expectedVal, int expectedCount) {
        int count = 0;
        while (iterator.hasNext()) {
            JsonNode jsonNode = iterator.next();
            Assert.assertEquals(expectedVal, jsonNode.get(key).asText());
            count++;
        }
        Assert.assertEquals(expectedCount, count);
    }

    public void topicAwareMatch(String topic, Iterator<JsonNode> iterator, String key, String expectedVal) {
        List<JsonNode> nodes = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                .filter(j -> filterByTopic(j, topic))
                .collect(Collectors.toList());
        match(nodes.iterator(), key, expectedVal, nodes.size());
    }

    public boolean filterByTopic(JsonNode node, String topic) {
        return node.get("metadata").get("topic").asText().equalsIgnoreCase(topic);
    }

    @Test
    public void testFlinkSerDeParing() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.readValue(
                    "{\"DLPMetaData\":[{\"topic\":\"mysql-test-1234-flink-test_2.rdstest.test_500gb\",\n" +
                            "\t\"columns\":[{\"column\":\"phone\",\n" +
                            "\t\t\"dlpTypes\":[{\"info_type\":\"PHONE_NUMBER\",\"mask_type\":\"REPLACE_CONFIG\",\n" +
                            "\t\t\"replace\":\"*\",\"salt\":\"\"}]}]}]}", FlinkCamouflageSerDe.class);
        } catch (Exception e) {
            Assert.fail("FlinkCamouflageSerDe parsin failure " + e.getMessage());
        }

    }

}

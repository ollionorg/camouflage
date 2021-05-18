package com.cldcvr.camouflage.flink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestBase {
    private static final List<String> cardTypes = Arrays.asList("VISA", "MASTER", "AMERICAN EXPRESS", "DISCOVER");
    protected final ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();
    protected StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

    public List<ObjectNode> generateTestRecords(String topic, int numRecords) {

        return IntStream.range(0, numRecords)
                .mapToObj(i -> dummyRecord(topic, i))
                .collect(Collectors.toList());
    }

    public ObjectNode dummyRecord(String topic, int id) {
        ObjectNode node = mapper.createObjectNode();
        node.put("id", id);
        node.put("name", "Tom-" + id);
        node.put("cardType", cardTypes.get(random.nextInt(cardTypes.size())));
        node.put("ssn", 7253474533L + id);
        node.put("phone", 86347349L + id);
        node.put("validTill", 500 + id);
        node.put("cvv", 550 + id);
        node.set("metadata", mapper.createObjectNode().put("topic", topic));
        return node;
    }

    protected void setCamouflageJsonToEnv(String[] args) {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String s = tool.get(MaskFlinkRecord.DLP_METADATA);
        System.out.println("SET " + s);
        env.getConfig().setGlobalJobParameters(tool);
    }

}

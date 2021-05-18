package com.cldcvr.camouflage.flink;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Serde takes a list of topics along with columns to mask
 * Example serde json
 * {
 * "DLPMetaData":[{
 * 	"topic":"mysql-test-1234-flink-test_2.rdstest.test_500gb",
 * 	"columns":[{
 * 		"column":"phone",
 * 		"dlpTypes":[{
 * 			"info_type":"PHONE_NUMBER",
 * 			"mask_type":"REDACT_CONFIG",
 * 			"replace":"*"
 *                }]* 	}]
 * },{
 * 	"topic":"mysql-test-1234-flink-test_3.rdstest.test_500gb",
 * 	"columns":[{
 * 		"column":"name",
 * 		"dlpTypes":[{
 * 			"info_type":"GENERIC",
 * 			"mask_type":"REDACT_CONFIG",
 * 			"replace":"*"
 *        }]* 	}]
 * }
 * ]
 * }
 */
public class FlinkCamouflageSerDe implements Serializable {
    private final List<TopicAndColumns> topicAndColumns;

    public FlinkCamouflageSerDe(@JsonProperty("DLPMetaData") List<TopicAndColumns> topicAndColumns)
    {
        this.topicAndColumns=topicAndColumns;
    }
    public List<TopicAndColumns> getList() {
        return topicAndColumns;
    }

    @Override
    public String toString() {
        return "SerDe{" +
                "topicAndColumns=" + topicAndColumns +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlinkCamouflageSerDe that = (FlinkCamouflageSerDe) o;
        return Objects.equals(topicAndColumns, that.topicAndColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicAndColumns);
    }
}


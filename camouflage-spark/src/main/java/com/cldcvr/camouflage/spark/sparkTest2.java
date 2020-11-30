package com.cldcvr.camouflage.spark;

import com.cldcvr.camouflage.spark.relation.CamouflageReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class sparkTest2 {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().master("local[*]").getOrCreate();
        //CamouflageReader.withSparkSession(session).
        Dataset<Row> camouflageDs = CamouflageReader.withSparkSession(session).format("json").withCamouflageJson("{\n" +
                "      \t\"DLPMetadata\":[{\n" +
                "      \t\t\"column\":\"id\",\n" +
                "      \t\t\"dlpTypes\":[{\n" +
                "      \t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                "                \"mask_type\":\"REDACT_CONFIG\",\n" +
                "                 \"replace\":\"*\"\n" +
                "                     }\n" +
                "             ] \t},\n" +
                "      \t{\n" +
                "      \t\t\"column\":\"ssn\",\n" +
                "      \t\t\"dlpTypes\":[{\n" +
                "      \t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                "                \"mask_type\":\"REDACT_CONFIG\",\n" +
                "                 \"replace\":\"somesaltgoeshere\"\n" +
                "      \t\t}\n" +
                "             ]\n" +
                "      \t}\n" +
                "      ]\n" +
                "      }")
                .withPrimaryKeysToIgnore("id","ssn")
                .load("/Users/taherkoitawala/git/camouflage/camouflage-spark/src/main/resources/input/emp.json")
                .getDataset();
        camouflageDs.show(100);
    }
}

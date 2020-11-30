package com.cldcvr.camouflage.spark;


import com.cldcvr.camouflage.spark.relation.CamouflageReader;
import com.cldcvr.camouflage.spark.relation.CamouflageWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> json =
        CamouflageReader.withSparkSession(session).withCamouflageJson(
                       "{\n" +
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
                                "                \"mask_type\":\"HASH_CONFIG\",\n" +
                                "                 \"salt\":\"IHAVEASPARKJOB\"\n" +
                                "      \t\t}\n" +
                                "             ]\n" +
                                "      \t}\n" +
                                "      ]\n" +
                                "      }")
                .format("json")
                .load("/Users/taherkoitawala/git/camouflage/camouflage-spark/src/main/resources/emp.json").getDataset();
        json.write().csv("/Users/taherkoitawala/git/camouflage/camouflage-spark/src/main/resources/csv/");

    }
}

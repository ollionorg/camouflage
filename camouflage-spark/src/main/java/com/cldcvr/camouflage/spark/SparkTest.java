package com.cldcvr.camouflage.spark;


import com.cldcvr.camouflage.spark.relation.CamouflageWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> json = session.read()
                .json("/Users/taherkoitawala/git/camouflage/camouflage-spark/src/main/resources/input/emp1.json");
        CamouflageWriter.builder().withDataSet(json).withCamouflageJson(
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
                                "                \"mask_type\":\"REDACT_CONFIG\",\n" +
                                "                 \"replace\":\"*\"\n" +
                                "      \t\t}\n" +
                                "             ]\n" +
                                "      \t}\n" +
                                "      ]\n" +
                                "      }")
                .format("csv")
                .mode(SaveMode.Overwrite)
                .save("/Users/taherkoitawala/git/camouflage/camouflage-spark/src/main/resources/csv/");

    }
}

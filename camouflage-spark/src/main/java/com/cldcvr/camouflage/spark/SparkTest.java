package com.cldcvr.camouflage.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[*]").getOrCreate();
        session.read().json("/Users/taherkoitawala/git/camouflage/camouflage-spark/src/main/resources/input/emp.json")
                        .write()
                        .format(CamouflageSource.NAME)
                        .option(CamouflageSource.OUTPUT_FORMAT,"csv")
                        .option(CamouflageSource.DLP_JSON,"{\n" +
                                "      \t\"DLPMetadata\":[{\n" +
                                "      \t\t\"column\":\"id\",\n" +
                                "      \t\t\"dlpTypes\":[{\n" +
                                "      \t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                                "                \"mask_type\":\"REDACT_CONFIG\",\n" +
                                "                 \"replace\":\"*\"\n" +
                                "                     }\n" +
                                "             ] \t},\n" +
                                "      \t{\n" +
                                "      \t\t\"column\":\"ssnt\",\n" +
                                "      \t\t\"dlpTypes\":[{\n" +
                                "      \t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                                "                \"mask_type\":\"HASH_CONFIG\",\n" +
                                "                 \"salt\":\"somesaltgoeshere\"\n" +
                                "      \t\t}\n" +
                                "             ]\n" +
                                "      \t}\n" +
                                "      ]\n" +
                                "      }")
                .mode(SaveMode.Overwrite)
                .save("/Users/taherkoitawala/git/camouflage/camouflage-spark/src/main/resources/csv/");
    }
}

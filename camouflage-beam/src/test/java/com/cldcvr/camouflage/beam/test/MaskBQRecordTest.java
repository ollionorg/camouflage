package com.cldcvr.camouflage.beam.test;
import com.cldcvr.camouflage.beam.*;
import com.cldcvr.camouflage.core.json.serde.CamouflageSerDe;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MaskBQRecordTest {
    private Instant instant = new DateTime("2019-11-08T22:01:59", DateTimeZone.UTC).toInstant();
    private BoundedWindow boundedWindow = new BoundedWindow() {
        @Override
        public Instant maxTimestamp() {
            return Instant.now();
        }
    };

    @Test
    public void TestAllowRecord() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        String json = "{\n" +
                "   \"DLPMetaData\":[\n" +
                "      {\n" +
                "         \"topic\":\"connect1_db_t1\",\n" +
                "         \"columns\":[\n" +
                "            {\n" +
                "               \"column\":\"card_number\",\n" +
                "               \"dlpTypes\":[\n" +
                "                  {\n" +
                "                     \"info_type\":\"PHONE_NUMBER\",\n" +
                "                     \"mask_type\":\"REDACT_CONFIG\",\n" +
                "                     \"replace\":\"*\"\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            {\n" +
                "               \"column\":\"card_pin\",\n" +
                "               \"dlpTypes\":[\n" +
                "                  {\n" +
                "                     \"info_type\":\"PHONE_NUMBER\",\n" +
                "                     \"mask_type\":\"HASH_CONFIG\",\n" +
                "                     \"salt\":\"somesaltgoeshere\"\n" +
                "                  }\n" +
                "               ]\n" +
                "            }\n" +
                "         ]\n" +
                "      },{\n" +
                "\"topic\":\"connect1.db_t2\",\n" +
                "         \"columns\":[\n" +
                "            {\n" +
                "               \"column\":\"card_number\",\n" +
                "               \"dlpTypes\":[\n" +
                "                  {\n" +
                "                     \"info_type\":\"PHONE_NUMBER\",\n" +
                "                     \"mask_type\":\"REDACT_CONFIG\",\n" +
                "                     \"replace\":\"*\"\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            {\n" +
                "               \"column\":\"card_pin\",\n" +
                "               \"dlpTypes\":[\n" +
                "                  {\n" +
                "                     \"info_type\":\"PHONE_NUMBER\",\n" +
                "                     \"mask_type\":\"HASH_CONFIG\",\n" +
                "                     \"salt\":\"somesaltgoeshere\"\n" +
                "                  }\n" +
                "               ]\n" +
                "            }\n" +
                "         ]\n" +
                "      }\n" +
                "   ]\n" +
                "}";

//        String json1 = "{\"DLPMetadata\":[{\"column\":\"card_number\",\"dlpTypes\":[{\"info_type\":\"PHONE_NUMBER\",\"mask_type\":\"REDACT_CONFIG\",\"replace\":\"*\"}]},{\"column\":\"card_pin\",\"dlpTypes\":[{\"info_type\":\"PHONE_NUMBER\",\"mask_type\":\"HASH_CONFIG\",\"salt\":\"somesaltgoeshere\"}]}]}";
        System.out.println(json);
//        CamouflageSerDe camouflageSerDe = mapper.readValue(json, CamouflageSerDe.class);
//        System.out.println("JSON " + camouflageSerDe);


        TableRow tblRow = new TableRow();
        tblRow.set("id", "151424995");
        tblRow.set("user_id", "90290100");
        tblRow.set("card_number", "4231944811234565");
        tblRow.set("card_pin", "1221");
        tblRow.set("_connector_name", "connect1");
        tblRow.set("_database_table","db_t1");


        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);
//        fnTester.startBundle();
//        fnTester.processWindowedElement(tblRow, instant, boundedWindow);
//        fnTester.finishBundle();

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        System.out.println(maskedRecord.toString());
        assertEquals(maskedRecord.get("card_number"),"****************");
//        Emp emp = mapper.readValue("{\"emp_name\":\"Taher\",\"emp_id\":\"1\"}",Emp.class);
//        System.out.println(emp);

    }

    @Test
    public void TestAllowRecord2() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        String json = "{\"DLPMetaData\": [{\"topic\": \"connect1_db_t1\",\"columns\": [{\"column\": \"SSN\",\"dlpTypes\": [{\"info_type\": \"US_SOCIAL_SECURITY_NUMBER\",\"mask_type\": \"REDACT_CONFIG\",\"replace\": \"*\"}]}]}]}";

//        String json1 = "{\"DLPMetadata\":[{\"column\":\"card_number\",\"dlpTypes\":[{\"info_type\":\"PHONE_NUMBER\",\"mask_type\":\"REDACT_CONFIG\",\"replace\":\"*\"}]},{\"column\":\"card_pin\",\"dlpTypes\":[{\"info_type\":\"PHONE_NUMBER\",\"mask_type\":\"HASH_CONFIG\",\"salt\":\"somesaltgoeshere\"}]}]}";
        System.out.println(json);
//        CamouflageSerDe camouflageSerDe = mapper.readValue(json, CamouflageSerDe.class);
//        System.out.println("JSON " + camouflageSerDe);


        TableRow tblRow = new TableRow();
        tblRow.set("id", "151424995");
        tblRow.set("SSN", "552-09-6781");
        tblRow.set("card_number", "4231944811234565");
        tblRow.set("card_pin", "1221");
        tblRow.set("_connector_name", "connect1");
        tblRow.set("_database_table","db_t1");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);
//        fnTester.startBundle();
//        fnTester.processWindowedElement(tblRow, instant, boundedWindow);
//        fnTester.finishBundle();

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        System.out.println(maskedRecord.toString());
        assertEquals(maskedRecord.get("SSN"),"***********");
//        Emp emp = mapper.readValue("{\"emp_name\":\"Taher\",\"emp_id\":\"1\"}",Emp.class);
//        System.out.println(emp);

    }


    static class Emp {
        private final String name;
        private final int id;

        Emp(@JsonProperty("emp_name") String name, @JsonProperty("emp_id") int id) {
            this.name = name;
            this.id = id;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "Emp{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }
    }
}

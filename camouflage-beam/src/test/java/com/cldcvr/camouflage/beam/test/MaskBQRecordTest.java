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
                "         \"topic\":\"connect1.db_t1\",\n" +
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
        String json = "{\"DLPMetaData\": [{\"topic\": \"connect1.db_t1\",\"columns\": [{\"column\": \"SSN\",\"dlpTypes\": [{\"info_type\": \"US_SOCIAL_SECURITY_NUMBER\",\"mask_type\": \"REDACT_CONFIG\",\"replace\": \"*\"}]}]}]}";

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

    @Test
    public void TestAllowRecord3() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-60589.rdstest.customer\", \"columns\": [{\"column\": \"event_time\", \"dlpTypes\": [{\"info_type\": \"TIME,DATE\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}, {\"column\": \"user_id\", \"dlpTypes\": [{\"info_type\": \"LOCATION\", \"mask_type\": \"HASH_CONFIG\", \"salt\": \"somesaltgoeshere\"}]}, {\"column\": \"brand\", \"dlpTypes\": [{\"info_type\": \"PERSON_NAME,LOCATION\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}]}, {\"topic\": \"rdstest-MYSQL-87-60589.rdstest.test_2gb\", \"columns\": [{\"column\": \"email\", \"dlpTypes\": [{\"info_type\": \"EMAIL_ADDRESS,PERSON_NAME,LOCATION\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}, {\"column\": \"name\", \"dlpTypes\": [{\"info_type\": \"PERSON_NAME,LOCATION\", \"mask_type\": \"HASH_CONFIG\", \"salt\": \"somesaltgoeshere\"}]}, {\"column\": \"download_speed\", \"dlpTypes\": [{\"info_type\": \"US_HEALTHCARE_NPI,LOCATION,IMEI_HARDWARE_ID,US_VEHICLE_IDENTIFICATION_NUMBER\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("id", 1);
        tblRow.set("department_code", "Data");
        tblRow.set("views", 5299);
        tblRow.set("download_speed", 8.6879403769856E13);
        tblRow.set("isCompromised", 0);
        tblRow.set("name","Karla Williams");
        tblRow.set("email","glenn44@yahoo.com");
        tblRow.set("created_at","2020-10-01T08:13:37Z");
        tblRow.set("_database_table","rdstest.test_2gb");
        tblRow.set("_connector_name","rdstest-MYSQL-87-60589");
        tblRow.set("_ts_ms","1606376167691");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        System.out.println(maskedRecord.toString());
        assertEquals("*****************",maskedRecord.get("email"));
        assertEquals(64, String.valueOf(maskedRecord.get("name")).length());
        assertEquals("******************", String.valueOf(maskedRecord.get("download_speed")));

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

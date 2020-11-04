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
                "\t\"DLPMetadata\":[{\n" +
                "\t\t\"column\":\"card_number\",\n" +
                "\t\t\"dlpTypes\":[{\n" +
                "\t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                "          \"mask_type\":\"REDACT_CONFIG\",\n" +
                "           \"replace\":\"*\"\n" +
                "\t\t}\n" +
                "       ]\n" +
                "\t},\n" +
                "\t{\n" +
                "\t\t\"column\":\"card_pin\",\n" +
                "\t\t\"dlpTypes\":[{\n" +
                "\t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                "          \"mask_type\":\"HASH\",\n" +
                "           \"salt\":\"somesaltgoeshere\"\n" +
                "\t\t}\n" +
                "       ]\n" +
                "\t}\n" +
                "]\n" +
                "}";

        String json1 = "{\"DLPMetadata\":[{\"column\":\"card_number\",\"dlpTypes\":[{\"info_type\":\"PHONE_NUMBER\",\"mask_type\":\"REDACT_CONFIG\",\"replace\":\"*\"}]},{\"column\":\"card_pin\",\"dlpTypes\":[{\"info_type\":\"PHONE_NUMBER\",\"mask_type\":\"HASH_CONFIG\",\"salt\":\"somesaltgoeshere\"}]}]}";
        System.out.println(json1);
        CamouflageSerDe camouflageSerDe = mapper.readValue(json1, CamouflageSerDe.class);
        System.out.println("JSON " + camouflageSerDe);


        TableRow tblRow = new TableRow();
        tblRow.set("id", "151424995");
        tblRow.set("user_id", "90290100");
        tblRow.set("card_number", "4231944811234565");
        tblRow.set("card_pin", "1221");


        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json1));
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

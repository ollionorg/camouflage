package com.cldcvr.camouflage.beam.test;
import com.cldcvr.camouflage.beam.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class MaskBQRecordTest {
    private Instant instant = new DateTime("2019-11-08T22:01:59", DateTimeZone.UTC).toInstant();
    private BoundedWindow boundedWindow = new BoundedWindow() {
        @Override
        public Instant maxTimestamp() {
            return Instant.now();
        }
    };
    private final Logger LOG = LoggerFactory.getLogger(MaskBQRecordTest.class);
    
    @Test
    public void TestRedactConfig() throws Exception {
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


        TableRow tblRow = new TableRow();
        tblRow.set("id", "151424995");
        tblRow.set("user_id", "90290100");
        tblRow.set("card_number", "4231944811234565");
        tblRow.set("card_pin", "1221");
        tblRow.set("_topic", "connect1.db_t1");


        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals(maskedRecord.get("card_number"),"****************");

    }

    @Test
    public void TestMaskRecordAgainstTopic() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        String json = "{\"DLPMetaData\": [{\"topic\": \"connect1.db_t1\",\"columns\": [{\"column\": \"SSN\",\"dlpTypes\": [{\"info_type\": \"US_SOCIAL_SECURITY_NUMBER\",\"mask_type\": \"REDACT_CONFIG\",\"replace\": \"*\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("id", "151424995");
        tblRow.set("SSN", "552-09-6781");
        tblRow.set("card_number", "4231944811234565");
        tblRow.set("card_pin", "1221");
        tblRow.set("_topic","connect1.db_t1");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);
        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("redact config should return same length string with replace character","***********",maskedRecord.get("SSN"));
    }

    @Test
    public void TestMaskRedactConfigForIntValues() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-60589.rdstest.customer\", \"columns\": [{\"column\": \"event_time\", \"dlpTypes\": [{\"info_type\": \"TIME,DATE\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}, {\"column\": \"user_id\", \"dlpTypes\": [{\"info_type\": \"LOCATION\", \"mask_type\": \"HASH_CONFIG\", \"salt\": \"somesaltgoeshere\"}]}, {\"column\": \"brand\", \"dlpTypes\": [{\"info_type\": \"PERSON_NAME,LOCATION\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}]}, {\"topic\": \"rdstest-MYSQL-87-60589.rdstest.test_2gb\", \"columns\": [{\"column\": \"email\", \"dlpTypes\": [{\"info_type\": \"EMAIL_ADDRESS,PERSON_NAME,LOCATION\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}, {\"column\": \"Name\", \"dlpTypes\": [{\"info_type\": \"PERSON_NAME,LOCATION\", \"mask_type\": \"HASH_CONFIG\", \"salt\": \"somesaltgoeshere\"}]}, {\"column\": \"download_speed\", \"dlpTypes\": [{\"info_type\": \"US_HEALTHCARE_NPI,LOCATION,IMEI_HARDWARE_ID,US_VEHICLE_IDENTIFICATION_NUMBER\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("id", 1);
        tblRow.set("department_code", "Data");
        tblRow.set("views", 5299);
        tblRow.set("download_speed", 8.6879403769856E13);
        tblRow.set("isCompromised", 0);
        tblRow.set("Name","Karla Williams");
        tblRow.set("email","glenn44@yahoo.com");
        tblRow.set("created_at","2020-10-01T08:13:37Z");
        tblRow.set("_topic","rdstest-MYSQL-87-60589.rdstest.test_2gb");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("*****************",maskedRecord.get("email"));
        assertEquals(64, String.valueOf(maskedRecord.get("Name")).length());
        assertEquals("******************", String.valueOf(maskedRecord.get("download_speed")));

    }

    @Test
    public void TestMaskingRcordWithUpperCase() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-54138.rdstest.customerdata\", \"columns\": [{\"column\": \"City\", \"dlpTypes\": [{\"info_type\": \"LOCATION,PERSON_NAME\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}, {\"column\": \"Name\", \"dlpTypes\": [{\"info_type\": \"PERSON_NAME\", \"mask_type\": \"HASH_CONFIG\", \"salt\": \"nikhil\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("entry_date", 1556928000);
        tblRow.set("Name", "Benedict");
        tblRow.set("Company", "Maecenas Foundation");
        tblRow.set("City", "Jamshoro");
        tblRow.set("id", 1);
        tblRow.set("entry_time","2020-08-07T10:22:10Z");
        tblRow.set("_topic","rdstest-MYSQL-87-54138.rdstest.customerdata");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("When column Name is in upper case but dlp meta has same column mentioned in lowercase, masking should apply","********",maskedRecord.get("City"));
        assertEquals("Hash config masking should return sting of length 64",64, String.valueOf(maskedRecord.get("Name")).length());
        assertEquals("Company should remain unmasked since no meta provided","Maecenas Foundation", String.valueOf(maskedRecord.get("Company")));

        // Null value test
        tblRow.set("City", null);
        tblRow.set("Name", "Benedict");
        fnTester.processElement(tblRow);
        maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("null value should be shown as it is in ouput",null,maskedRecord.get("City"));
        assertEquals("Hash config masking should return sting of length 64",64, String.valueOf(maskedRecord.get("Name")).length());
    }

    @Test
    public void TestMaskingRcordWithBooleanValue() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-54138.rdstest.customerdata\", \"columns\": [{\"column\": \"City\", \"dlpTypes\": [{\"info_type\": \"LOCATION,PERSON_NAME\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}, {\"column\": \"Name\", \"dlpTypes\": [{\"info_type\": \"PERSON_NAME\", \"mask_type\": \"HASH_CONFIG\", \"salt\": \"nikhil\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("entry_date", 1556928000);
        tblRow.set("Name", "Benedict");
        tblRow.set("Company", "Maecenas Foundation");
        tblRow.set("City", true);
        tblRow.set("id", 1);
        tblRow.set("entry_time","2020-08-07T10:22:10Z");
        tblRow.set("_topic","rdstest-MYSQL-87-54138.rdstest.customerdata");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("Hash config masking should return sting of length 64",64, String.valueOf(maskedRecord.get("Name")).length());
        assertEquals("When column Name is in upper case but dlp meta has same column mentioned in lowercase, masking should apply","****",maskedRecord.get("City"));
        assertEquals("Company should remain unmasked since no meta provided","Maecenas Foundation", String.valueOf(maskedRecord.get("Company")));

    }

    @Test
    public void TestHashConfigWithNoSalt() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-54138.rdstest.customerdata\", \"columns\": [{\"column\": \"City\", \"dlpTypes\": [{\"info_type\": \"LOCATION,PERSON_NAME\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*\"}]}, {\"column\": \"Name\", \"dlpTypes\": [{\"info_type\": \"PERSON_NAME\", \"mask_type\": \"HASH_CONFIG\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("Name", "Benedict");
        tblRow.set("Company", "Maecenas Foundation");
        tblRow.set("City", true);
        tblRow.set("_topic","rdstest-MYSQL-87-54138.rdstest.customerdata");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("Hash config masking should return sting of length 64",64, String.valueOf(maskedRecord.get("Name")).length());
        assertEquals("When column Name is in upper case but dlp meta has same column mentioned in lowercase, masking should apply","****",maskedRecord.get("City"));
        assertEquals("Company should remain unmasked since no meta provided","Maecenas Foundation", String.valueOf(maskedRecord.get("Company")));

    }

    @Test
    public void TestRedactConfigWithNoReplaceChar() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-54138.rdstest.customerdata\", \"columns\": [{\"column\": \"City\", \"dlpTypes\": [{\"info_type\": \"LOCATION,PERSON_NAME\", \"mask_type\": \"REDACT_CONFIG\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("Name", "Benedict");
        tblRow.set("Company", "Maecenas Foundation");
        tblRow.set("City", "New York");
        tblRow.set("_topic","rdstest-MYSQL-87-54138.rdstest.customerdata");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("When column Name is in upper case but dlp meta has same column mentioned in lowercase, masking should apply","********",maskedRecord.get("City"));
        assertEquals("Company should remain unmasked since no meta provided","Maecenas Foundation", String.valueOf(maskedRecord.get("Company")));

    }

    @Test
    public void TestRedactConfigWithReplaceCharMoreThanOne() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-54138.rdstest.customerdata\", \"columns\": [{\"column\": \"City\", \"dlpTypes\": [{\"info_type\": \"LOCATION,PERSON_NAME\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"*#\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("Name", "Benedict");
        tblRow.set("Company", "Maecenas Foundation");
        tblRow.set("City", "New York");
        tblRow.set("_topic","rdstest-MYSQL-87-54138.rdstest.customerdata");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("When column Name is in upper case but dlp meta has same column mentioned in lowercase, masking should apply","********",maskedRecord.get("City"));
        assertEquals("Company should remain unmasked since no meta provided","Maecenas Foundation", String.valueOf(maskedRecord.get("Company")));

    }

    @Test
    public void TestRedactConfigWithReplaceCharEmpty() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-54138.rdstest.customerdata\", \"columns\": [{\"column\": \"City\", \"dlpTypes\": [{\"info_type\": \"LOCATION,PERSON_NAME\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("Name", "Benedict");
        tblRow.set("Company", "Maecenas Foundation");
        tblRow.set("City", "New York");
        tblRow.set("_topic","rdstest-MYSQL-87-54138.rdstest.customerdata");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("When column Name is in upper case but dlp meta has same column mentioned in lowercase, masking should apply","********",maskedRecord.get("City"));
        assertEquals("Company should remain unmasked since no meta provided","Maecenas Foundation", String.valueOf(maskedRecord.get("Company")));

    }

    @Test
    public void TestRecordWithNoValidTopic() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-541380.rdstest.customerdata\", \"columns\": [{\"column\": \"City\", \"dlpTypes\": [{\"info_type\": \"LOCATION,PERSON_NAME\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("Name", "Benedict");
        tblRow.set("Company", "Maecenas Foundation");
        tblRow.set("City", "New York");
        tblRow.set("_topic","rdstest-MYSQL-87-54138.rdstest.customerdata");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("Record should remain unaffected","New York",maskedRecord.get("City"));
        assertEquals("Record should remain unaffected","Maecenas Foundation", String.valueOf(maskedRecord.get("Company")));

    }

    @Test
    public void TestRecordWithNoColToMask() throws Exception {
        String json = "{\"DLPMetaData\": [{\"topic\": \"rdstest-MYSQL-87-54138.rdstest.customerdata\", \"columns\": [{\"column\": \"City\", \"dlpTypes\": [{\"info_type\": \"LOCATION,PERSON_NAME\", \"mask_type\": \"REDACT_CONFIG\", \"replace\": \"\"}]}]}]}";

        TableRow tblRow = new TableRow();
        tblRow.set("Name1", "Benedict");
        tblRow.set("Company1", "Maecenas Foundation");
        tblRow.set("City1", "New York");
        tblRow.set("_topic","rdstest-MYSQL-87-54138.rdstest.customerdata");

        DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(new MaskBQRecord(json));
        fnTester.processElement(tblRow);

        TableRow maskedRecord = fnTester.takeOutputElements().get(0);
        LOG.info(maskedRecord.toString());
        assertEquals("Record should remain unaffected","New York",maskedRecord.get("City1"));
        assertEquals("Record should remain unaffected","Maecenas Foundation", String.valueOf(maskedRecord.get("Company1")));

    }
}

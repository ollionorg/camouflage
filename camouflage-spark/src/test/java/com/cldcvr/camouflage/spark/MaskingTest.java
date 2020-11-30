package com.cldcvr.camouflage.spark;

import com.cldcvr.camouflage.core.json.serde.CamouflageSerDe;
import com.cldcvr.camouflage.core.json.serde.ColumnMetadata;
import com.cldcvr.camouflage.core.json.serde.TypeMetadata;
import com.cldcvr.camouflage.spark.relation.CamouflageReader;
import com.cldcvr.camouflage.spark.util.TestUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.cldcvr.camouflage.spark.util.TestUtils.*;

public class MaskingTest {

    @Test
    public void testPhoneNumberConfig() {
        int numRecords = 2;
        writeDataSet(getTestRecords(numRecords), inputPath, PARQUET);
        CamouflageSerDe testSerde = new CamouflageSerDe(Arrays.asList(new ColumnMetadata("phone", Arrays.asList(new TypeMetadata("PHONE_NUMBER", "REPLACE_CONFIG",
                "*", "")))));
        String json = toJson(testSerde);
        Dataset<Row> dataset = CamouflageReader.withSparkSession(getSession()).withCamouflageJson(json).format(PARQUET).load(inputPath).getDataset();
        List<String> actual = dataset.select("phone").collectAsList().stream().map(r -> r.get(0) + "").collect(Collectors.toList());
        long assertion = actual.stream().filter(v -> {
            if (v != null) {
                Assert.assertEquals("********", v);
                System.out.println("if " + v);
                return true;
            } else {
                System.out.println("else " + v);
                return false;
            }
        }).count();
        Assert.assertEquals(numRecords, assertion);
    }

    @Test
    public void testPhoneConfigWithPartiallyNullDataset() {
        int numRecords = 10;
        Dataset<Row> partiallyNull = getPartiallyNull(10,"phone");
        partiallyNull.show();
        long count = partiallyNull.select("phone").filter("phone IS NULL").count();
        System.out.println(count);
        writeDataSet(partiallyNull, inputPath, PARQUET);
        CamouflageSerDe testSerde = new CamouflageSerDe(Arrays.asList(new ColumnMetadata("phone", Arrays.asList(new TypeMetadata("PHONE_NUMBER", "REPLACE_CONFIG",
                "*", "")))));
        String json = toJson(testSerde);
        Dataset<Row> dataset = CamouflageReader.withSparkSession(getSession()).withCamouflageJson(json).format(PARQUET).load(inputPath).getDataset();
        dataset.show();
        List<String> actual = dataset.select("phone").collectAsList().stream().map(r -> r.get(0) + "").collect(Collectors.toList());
        long assertion = actual.stream().filter(v -> {
            if (v != null) {
                Assert.assertEquals("********", v);
                System.out.println("if " + v);
                return true;
            } else {
                System.out.println("else " + v);
                return false;
            }
        }).count();
        Assert.assertEquals(numRecords, assertion);

    }




    public String toJson(CamouflageSerDe serDe) {
        try {
            return TestUtils.mapper.writeValueAsString(serDe);
        } catch (Exception e) {
            Assert.fail("Error parsing the CamouflageSerDe object to serde \n" + e.getMessage());
            return null;
        }
    }
}

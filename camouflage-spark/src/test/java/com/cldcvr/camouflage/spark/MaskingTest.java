package com.cldcvr.camouflage.spark;

import com.cldcvr.camouflage.core.json.serde.CamouflageSerDe;
import com.cldcvr.camouflage.core.json.serde.ColumnMetadata;
import com.cldcvr.camouflage.core.json.serde.TypeMetadata;
import com.cldcvr.camouflage.core.mask.types.impl.HashConfig;
import com.cldcvr.camouflage.spark.relation.CamouflageReader;
import com.cldcvr.camouflage.spark.util.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.cldcvr.camouflage.spark.util.TestUtils.*;

public class MaskingTest {

    private final String IS_NULL = "%s IS NULL";
    private final String IS_NOT_NULL = "%s IS NOT NULL";

    @Test
    public void testPhoneNumberConfig() {
        int numRecords = 2;
        writeDataSet(getTestRecords(numRecords), inputPath, PARQUET);
        CamouflageSerDe testSerde = new CamouflageSerDe(Arrays.asList(new ColumnMetadata("phone", Arrays.asList(new TypeMetadata("PHONE_NUMBER", "REPLACE_CONFIG",
                "*", "")))));
        Dataset<Row> dataset = read(toJson(testSerde));
        List<String> actual = dataset.select("phone").collectAsList().stream().map(r -> r.get(0) + "").collect(Collectors.toList());
        long assertion = assertAndCount(actual, "********");
        Assert.assertEquals(numRecords, assertion);
    }

    @Test
    public void testWithPartiallyNullDataset() {
        int numRecords = 10;
        Dataset<Row> partiallyNull = getPartiallyNull(10, "phone").cache();
        long nullRecords = partiallyNull.select("phone").filter(String.format(IS_NULL, "phone")).count();
        writeDataSet(partiallyNull, inputPath, PARQUET);
        CamouflageSerDe testSerde = new CamouflageSerDe(Arrays.asList(new ColumnMetadata("phone", Arrays.asList(new TypeMetadata("PHONE_NUMBER", "REPLACE_CONFIG",
                "*", "")))));
        Dataset<Row> dataset = read(toJson(testSerde));
        List<String> actual = dataset.select("phone").filter(String.format(IS_NOT_NULL, "phone")).collectAsList().stream().map(r -> r.get(0) + "").collect(Collectors.toList());
        long assertion = assertAndCount(actual, "********");
        Assert.assertEquals(numRecords - nullRecords, assertion);
    }

    private Dataset<Row> read(String json) {
        return CamouflageReader.withSparkSession(getSession()).withCamouflageJson(json).format(PARQUET).load(inputPath).getDataset();
    }

    private long assertAndCount(List<String> actual, String expectedVal) {
        return actual.stream().filter(v -> {
            if (v != null) {
                Assert.assertEquals(expectedVal, v);
                return true;
            } else {
                return false;
            }
        }).count();
    }


    @Test
    public void testWithMultipleMaskConfig() {
        int numRecords = 10;
        String salt = "THIS_IS_SALTY";
        Dataset<Row> partiallyNull = getPartiallyNull(numRecords, "cvv", "ssn", "cardNumber");
        partiallyNull.cache();
        writeDataSet(partiallyNull, inputPath, PARQUET);
        CamouflageSerDe testSerde = new CamouflageSerDe(Arrays.asList(new ColumnMetadata("cvv", Arrays.asList(new TypeMetadata("PHONE_NUMBER", "REPLACE_CONFIG",
                        "*", ""))), new ColumnMetadata("ssn", Arrays.asList(new TypeMetadata("SSN", "HASH_CONFIG", "", salt)))
                , new ColumnMetadata("cardNumber", Arrays.asList(new TypeMetadata("PII", "REDACT_CONFIG", "*", "")))));
        Dataset<Row> dataset = read(toJson(testSerde));

        List<String> cvvList = rowToString(dataset.select("cvv").filter(String.format(IS_NOT_NULL, "cvv")).collectAsList());
        long cvvAssertion = assertAndCount(cvvList, "***");
        long cvvNull = partiallyNull.select("cvv").filter(String.format(IS_NULL, "cvv")).count();
        Assert.assertEquals(numRecords - cvvNull, cvvAssertion);

        List<String> cardNumberList = rowToString(dataset.select("cardNumber").filter(String.format(IS_NOT_NULL, "cardNumber")).collectAsList());
        long cardNumberAssertion = assertAndCount(cardNumberList, "***********");
        long cardNumberNulls = partiallyNull.select("cardNumber").filter(String.format(IS_NULL, "cardNumber")).count();
        Assert.assertEquals(numRecords - cardNumberNulls, cardNumberAssertion);

        Set<String> ssnActual = rowToString(dataset.select("ssn").filter(String.format(IS_NOT_NULL, "ssn")).collectAsList()).stream().collect(Collectors.toSet());
        long ssnAssertion = ssnActual.stream().filter(f -> f.length() == 64).count();
        long ssnNulls = partiallyNull.select("ssn").filter(String.format(IS_NULL, "ssn")).count();
        Set<String> ssnExpected = rowToString(partiallyNull.select("ssn").filter(String.format(IS_NOT_NULL, "ssn")).collectAsList())
                .stream().map(v -> new HashConfig(salt).applyMaskStrategy(v, "")).collect(Collectors.toSet());
        Assert.assertEquals(ssnActual, ssnExpected);
        Assert.assertEquals(numRecords - ssnNulls, ssnAssertion);
    }

    public List<String> rowToString(List<Row> list) {
        return list.stream().map(r -> r.get(0) + "").collect(Collectors.toList());
    }

    public String toJson(CamouflageSerDe serDe) {
        try {
            return TestUtils.mapper.writeValueAsString(serDe);
        } catch (Exception e) {
            Assert.fail("Error parsing the CamouflageSerDe object to serde \n" + e.getMessage());
            return null;
        }
    }

    @After
    public void clean() {
        TestUtils.clean(testPath);
    }
}
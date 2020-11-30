package com.cldcvr.camouflage.spark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.StructField;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestUtils {

    private static final List<String> cardTypes = Arrays.asList("VISA", "MASTER");
    private static final Random random = new Random();
    public static final ObjectMapper mapper = new ObjectMapper();
    public static final String testPath = "test";
    public static final String inputPath = testPath + "/input/";
    public static final String outputPath = testPath + "/output/";
    public static final String assertPath = testPath + "/assert/";
    public static final String PARQUET = "PARQUET";
    public static final UDF1<Object, Object> nullableUdf = o -> {
        int val = random.nextInt(2);
        return val % 2 == 0 ? o : null;
    };


    public static SparkSession getSession() {
        return SparkSession.builder().master("local[*]").getOrCreate();
    }

    public static void clean(String path) {
        File file = new File(path);
        if (file.isDirectory()) {
            Stream.of(file.listFiles()).map(r -> r.getPath()).forEach(TestUtils::clean);
        } else {
            file.delete();
        }
    }

    public static Dataset<Row> getTestRecords(int numRecords) {
        List<CreditCardDetails> records =
                IntStream.range(0, numRecords).mapToObj(r ->
                        new CreditCardDetails("Tom-" + r + r, cardTypes.get(random.nextInt(cardTypes.size())),
                                "7147732324" + r, 7253474533L + r, 86347349L + r, "", 550 + r))
                        .collect(Collectors.toList());
        return getSession().createDataFrame(records, CreditCardDetails.class);
    }

    public static Dataset<Row> getPartiallyNull(int numRecord, String... colsToNull) {
        Dataset<Row> testRecords = getTestRecords(numRecord);
        AtomicReference<Dataset<Row>> temp = new AtomicReference<>(testRecords);
        List<String> cols = Arrays.asList(colsToNull);
        Arrays.stream(testRecords.schema().fields())
                .map(StructField::name)
                .filter(cols::contains)
                .forEach(f -> {
                    StructField structField = Arrays.stream(testRecords.schema().fields()).filter(s -> s.name().equals(f)).findFirst().get();
                    getSession().sqlContext().udf().register(f + "_nullable", nullableUdf, structField.dataType());
                    temp.set(temp.get().withColumn(f, functions.callUDF(f + "_nullable", functions.col(f))));
                });
        return temp.get();
    }

    public static void writeDataSet(Dataset<Row> dataSet, String path, String format) {
        Objects.requireNonNull(dataSet).write().format(format).mode(SaveMode.Overwrite).save(path);
    }

    public static class CreditCardDetails {
        private final String cardHolderName;
        private final String cardType;
        private final String cardNumber;
        private final Long ssn;
        private final Long phone;
        private final String validTill;
        private final Integer cvv;

        public CreditCardDetails(String cardHolderName, String cardType, String cardNumber, Long ssn, Long phone, String validTill, Integer cvv) {
            this.cardHolderName = cardHolderName;
            this.cardType = cardType;
            this.cardNumber = cardNumber;
            this.ssn = ssn;
            this.phone = phone;
            this.validTill = validTill;
            this.cvv = cvv;
        }

        public String getCardHolderName() {
            return cardHolderName;
        }

        public String getCardType() {
            return cardType;
        }

        public String getCardNumber() {
            return cardNumber;
        }

        public Long getSsn() {
            return ssn;
        }

        public Long getPhone() {
            return phone;
        }

        public String getValidTill() {
            return validTill;
        }

        public Integer getCvv() {
            return cvv;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CreditCardDetails that = (CreditCardDetails) o;
            return Objects.equals(cardHolderName, that.cardHolderName) &&
                    Objects.equals(cardType, that.cardType) &&
                    Objects.equals(cardNumber, that.cardNumber) &&
                    Objects.equals(ssn, that.ssn) &&
                    Objects.equals(phone, that.phone) &&
                    Objects.equals(validTill, that.validTill) &&
                    Objects.equals(cvv, that.cvv);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cardHolderName, cardType, cardNumber, ssn, phone, validTill, cvv);
        }
    }
}

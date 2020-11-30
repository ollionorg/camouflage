package com.cldcvr.camouflage.spark.relation;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.immutable.Map;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.cldcvr.camouflage.spark.relation.Util.checkOrThrow;

public class CamouflageReader {

    private final Dataset<Row> dataset;

    private CamouflageReader(SparkSession session, String json, String format, String path, Map<String, String> extraOptions, String[] keysToIgnore) {
        checkOrThrow(json == null || json.equals(""), "CamouflageJson cannot be null or empty");
        checkOrThrow(path == null, "Input path cannot be null or empty");
        checkOrThrow(format == null || format.equals(""), "Spark file format cannot be null or empty");
        this.dataset = session.read().format(CamouflageSource.NAME)
                .option(CamouflageSource.FORMAT, format)
                .option(CamouflageSource.JSON, json)
                .option(CamouflageSource.PRIMARY_KEYS_TO_IGNORE_DLP_ON, keysToIgnore == null ? "" : Arrays.stream(keysToIgnore).collect(Collectors.joining(",")))
                .options(extraOptions)
                .load(path);
    }

    public Dataset<Row> getDataset() {
        return dataset;
    }

    public static Builder withSparkSession(SparkSession session) {
        if (session == null) {
            throw new IllegalArgumentException("Spark session cannot be null");
        }
        return new Builder(session);
    }

    public static class Builder {
        private String json = null;
        private String format = null;
        private String[] primaryKeys = null;
        private Map<String, String> options = scala.collection.immutable.Map$.MODULE$.<String, String>empty();
        private final SparkSession session;

        private Builder(SparkSession session) {
            this.session = session;
        }

        public Builder withCamouflageJson(String json) {
            this.json = json;
            return this;
        }

        public Builder withPrimaryKeysToIgnore(String... keys) {
            this.primaryKeys = keys;
            return this;
        }

        public Builder format(String format) {
            this.format = format;
            return this;
        }

        public Builder option(String key, String value) {
            options = options.$plus(Tuple2.apply(key, value));
            return this;
        }

        public CamouflageReader load(String path) {
            return new CamouflageReader(session, json, format, path, options, primaryKeys);
        }
    }
}
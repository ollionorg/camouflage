package com.cldcvr.camouflage.spark.relation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.cldcvr.camouflage.spark.relation.Util.checkOrThrow;

public class CamouflageWriter {

    public static Builder builder() {
        return new Builder();
    }

    private CamouflageWriter(Dataset<Row> dataset, String path, String json, SaveMode mode, String[] partitionColumns, String format) {
        checkOrThrow(dataset == null, "Dataset cannot be null or empty");
        checkOrThrow(json == null || json.equals(""), "CamouflageJson cannot be null or empty");
        checkOrThrow(path == null || path.equals(""), "Output path cannot be null or empty");
        checkOrThrow(format == null || format.equals(""), "Spark file format cannot be null or empty");
        dataset.write().format(CamouflageSource.NAME)
                .option(CamouflageSource.JSON, json)
                .option(CamouflageSource.FORMAT, format)
                .option(CamouflageSource.PARTITION_BY, partitionColumns == null || partitionColumns.length == 0
                        ? "" : Arrays.stream(partitionColumns).collect(Collectors.joining(",")))
                .mode(mode == null ? SaveMode.ErrorIfExists : mode)
                .save(path);


    }

    public static class Builder {
        private Dataset<Row> dataset = null;
        private String json = null;
        private SaveMode mode = null;
        private String[] partitionColumns = null;
        private String format = null;
        private String path = null;

        private Builder() {
        }

        public Builder withCamouflageJson(String json) {
            this.json = json;
            return this;
        }

        public Builder withDataSet(Dataset<Row> dataset) {
            this.dataset = dataset;
            return this;
        }

        public Builder mode(SaveMode mode) {
            this.mode = mode;
            return this;
        }

        public Builder partitionBy(String... partitionColumns) {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder format(String format) {
            this.format = format;
            return this;
        }

        public CamouflageWriter save(String path) {
            return new CamouflageWriter(dataset, path, json, mode, partitionColumns, format);
        }
    }


}

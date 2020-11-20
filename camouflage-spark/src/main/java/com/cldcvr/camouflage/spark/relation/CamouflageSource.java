package com.cldcvr.camouflage.spark.relation;

import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.json.serde.CamouflageSerDe;
import com.cldcvr.camouflage.core.json.serde.ColumnMetadata;
import com.cldcvr.camouflage.core.util.MapToInfoType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.collection.JavaConversions;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CamouflageSource implements DataSourceRegister, CreatableRelationProvider, Serializable, DataSourceV2, RelationProvider {

    transient private final ObjectMapper mapper = new ObjectMapper();
    public final static String NAME = "com.cldcvr.camouflage.spark.relation.CamouflageSource";
    public final static String JSON = "JSON";
    public final static String FORMAT = "FORMAT";
    public final static String PRIMARY_KEYS_TO_IGNORE_DLP_ON = "PRIMARY_KEYS_TO_IGNORE_DLP_ON";
    public final static String PARTITION_BY = "PARTITION_BY";
    private static final Logger LOG = LoggerFactory.getLogger(CamouflageSource.class);


    @Override
    public String shortName() {
        return "camouflage";
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        LOG.info("Called with writer");
        try {
            data = process(sqlContext, JavaConversions.mapAsJavaMap(parameters), data);
            CamouflageBaseRelation camouflageBaseRelation = new CamouflageBaseRelation(data, data.sqlContext());
            write(data, parameters, mode);
            return camouflageBaseRelation;
        } catch (IOException | CamouflageApiException e) {
            throw new RuntimeException(e);
        }
    }

    private void validations(java.util.Map<String, String> parameters, CamouflageSerDe camouflageSerDe) {
        String partitionCols = parameters.get(PARTITION_BY);
        String primaryKeyCols = parameters.get(PRIMARY_KEYS_TO_IGNORE_DLP_ON);
        if (partitionCols != null && !partitionCols.equals("")) {
            validate(camouflageSerDe, partitionCols, "[%s] columns cannot be part of partition by clause as DLP is being applied on them");
        }
        if (primaryKeyCols != null && !primaryKeyCols.equals("")) {
            validate(camouflageSerDe, primaryKeyCols, "[%s] columns cannot be part of DLP as they are marked as primary key columns via withPrimaryKeysToIgnore() method ");
        }
    }

    private void validate(CamouflageSerDe camouflageSerDe, String cols, String message) {
        List<String> errorColumns = camouflageSerDe.getDlpMetadata().stream().map(ColumnMetadata::getColumn).filter(
                r -> Arrays.asList(cols.split(",")).contains(r)
        ).collect(Collectors.toList());
        if (!errorColumns.isEmpty()) {
            throw new IllegalArgumentException(String.format(message, errorColumns));
        }
    }

    private Dataset<Row> process(SQLContext sqlContext, java.util.Map<String, String> parameters, Dataset<Row> data) throws CamouflageApiException, IOException {
        LOG.info("THIS IS THE NEW LINE WE WANTED TO ADD");
        StructType schema = data.schema();
        CamouflageSerDe camouflageSerDe = mapper.readValue(parameters.get(JSON), CamouflageSerDe.class);
        validations(parameters, camouflageSerDe);
        for (int i = 0; i < camouflageSerDe.getDlpMetadata().size(); i++) {
            ColumnMetadata columnMetadata = camouflageSerDe.getDlpMetadata().get(i);
            String columnName = columnMetadata.getColumn();
            Set<AbstractInfoType> abstractInfoTypes = MapToInfoType.toInfoTypeMapping(columnMetadata.getDlpTypes());
            for (AbstractInfoType abstractInfoType : abstractInfoTypes) {
                UDF1<String, String> udf = (String s) -> new Applicator(abstractInfoType).getFunction().apply(s);
                String udfName = String.format("%s_%s_UDF", columnName, abstractInfoType.toString());
                sqlContext.udf().register(udfName, udf, DataTypes.StringType);
                Arrays.stream(schema.fields()).map(StructField::name).filter(f -> f.equalsIgnoreCase(columnName)).findFirst()
                        .orElseThrow(() -> new IllegalArgumentException(String.format("`%s` column does not exits in spark dataset.\n " +
                                String.format("Dataset schema is %s", schema), columnName)));
                LOG.info("Map Parameters" +parameters);
                data = data.withColumn(columnName + "_UDF", functions.callUDF(udfName, functions.col(columnName).cast(DataTypes.StringType)))
                        .drop(columnName).withColumnRenamed(columnName + "_UDF", columnName);
                break;
            }
        }
        return data;
    }

    private void write(Dataset<Row> dataset, Map<String, String> parameters, SaveMode mode) {
        String format = parameters.get(FORMAT).get();
        String path = parameters.get("path").get();
        String partitionBy = parameters.get(PRIMARY_KEYS_TO_IGNORE_DLP_ON).get();
        DataFrameWriter<Row> dataFrameWriter = dataset.write().format(format);
        if (!partitionBy.equals("")) {
            dataFrameWriter.partitionBy(partitionBy.split(","));
        }
        dataFrameWriter.mode(mode).save(path);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        try {
            System.out.println("Create Relation called");
            System.out.println(parameters);
            Dataset<Row> dataset = sqlContext.read().options(parameters).format(parameters.get(CamouflageSource.FORMAT).get())
                    .load(parameters.get("path").get().split(","));
            Dataset<Row> df = process(sqlContext, JavaConversions.mapAsJavaMap(parameters), dataset);
            return new CamouflageBaseRelation(df, sqlContext);
        } catch (CamouflageApiException | IOException e) {
            throw new RuntimeException("Failed to read data to Camouflage format", e);
        }
    }

    private static final class Applicator implements Serializable {
        private final AbstractInfoType abstractInfoType;

        Applicator(AbstractInfoType abstractInfoType) {
            this.abstractInfoType = abstractInfoType;
        }

        Function<String, String> getFunction() {
            return data -> abstractInfoType.getMaskStrategy().applyMaskStrategy(data, abstractInfoType.regex());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Applicator that = (Applicator) o;
            return Objects.equals(abstractInfoType, that.abstractInfoType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(abstractInfoType);
        }

        @Override
        public String toString() {
            return abstractInfoType.name() + "_" + abstractInfoType.getMaskStrategy().name() + "_" + abstractInfoType.getMaskStrategy().hashCode() + "_" + abstractInfoType.hashCode();
        }
    }


}


package com.cldcvr.camouflage.spark;

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
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Function0;
import scala.Serializable;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class CamouflageSource implements DataSourceRegister, CreatableRelationProvider, Serializable, DataSourceV2 {

    transient private final ObjectMapper  mapper =  new ObjectMapper();
    public final static String NAME = "com.cldcvr.camouflage.spark.CamouflageSource";
    public final static String DLP_JSON = "DLP_JSON";
    public final static String OUTPUT_FORMAT = "format";

    @Override
    public String shortName() {
        return "camouflage";
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        try{
            String outputFormat = parameters.get(DLP_JSON).getOrElse(null);
            if(outputFormat==null || outputFormat.equals(""))
            {
                throw new IllegalArgumentException("DLP_JSON needed");
            }
            StructType schema = data.schema();
            CamouflageSerDe camouflageSerDe = mapper.readValue(parameters.get(DLP_JSON).get(), CamouflageSerDe.class);
            final java.util.Map<String,Set<String>> columnUdfMap =  new HashMap<>();

            for (int i=0;i<camouflageSerDe.getDlpMetadata().size();i++)
            {
                ColumnMetadata columnMetadata = camouflageSerDe.getDlpMetadata().get(i);
                String columnName = columnMetadata.getColumn();
                Set<AbstractInfoType> abstractInfoTypes = MapToInfoType.toInfoTypeMapping(columnName, columnMetadata.getDlpTypes());
                Iterator<AbstractInfoType> iterator = abstractInfoTypes.iterator();
                Set<String> udfSet = new LinkedHashSet<>();
                while (iterator.hasNext())
                {
                    AbstractInfoType abstractInfoType  = iterator.next();
                    UDF1<String,String> udf =  (String s) -> new Applicator(abstractInfoType).getFunction().apply(s);
                    String udfName = String.format("%s_%s_UDF", columnName,abstractInfoType.toString());
                    sqlContext.udf().register(udfName,udf, DataTypes.StringType);
                    udfSet.add(udfName);
                    Arrays.stream(schema.fields()).map(s->s.name()).filter(f->f.equalsIgnoreCase(columnName)).findFirst()
                            .orElseThrow(()->new IllegalArgumentException(String.format("`%s` column does not exits in spark dataset.\n " +
                                    String.format("Dataset schema is %s", schema), columnName)));
                    data = data.withColumn(columnName+"_UDF",functions.callUDF(udfName,functions.col(columnName)))
                    .drop(columnName).withColumnRenamed(columnName+"_UDF",columnName);
                }
                if(columnUdfMap.containsKey(columnName))
                {
                    Set<String> udfs = columnUdfMap.get(columnName);
                    udfs.addAll(udfSet);
                    columnUdfMap.put(columnName,udfs);
                }
                else {
                    columnUdfMap.put(columnName,udfSet);
                }
            }
        } catch (IOException | CamouflageApiException e) {
            e.printStackTrace();
        }
        CamouflageBaseRelation camouflageBaseRelation = new CamouflageBaseRelation(data.sqlContext(),data.schema());
        data.write().format(parameters.get(OUTPUT_FORMAT).get()).mode(mode).save(parameters.get("path").get());
        return camouflageBaseRelation;
    }


    private final class CamouflageBaseRelation extends BaseRelation implements Serializable{

        private final SQLContext context;
        private final StructType schema;
        public CamouflageBaseRelation(SQLContext context, StructType schema)
        {
            this.context=context;
            this.schema=schema;
        }
        @Override
        public SQLContext sqlContext() {
            return this.context;
        }
        @Override
        public StructType schema() {
            return this.schema;
        }
    }

    final class Applicator implements Serializable
    {
        private final AbstractInfoType abstractInfoType;
        Applicator(AbstractInfoType abstractInfoType)
        {
         this.abstractInfoType=abstractInfoType;
        }

        public Function<String, String> getFunction()
        {
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
            return abstractInfoType.name()+"_"+abstractInfoType.getMaskStrategy().name()+"_"+abstractInfoType.getMaskStrategy().hashCode()+"_"+abstractInfoType.hashCode();
        }
    }
}


package com.cldcvr.camouflage.spark.relation;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;

final class CamouflageBaseRelation extends BaseRelation implements Serializable, TableScan {

    private final SQLContext context;
    private final StructType schema;
    private final Dataset<Row> dataset;
    public CamouflageBaseRelation(Dataset<Row> dataset, SQLContext context, StructType schema)
    {
        this.dataset=dataset;
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

    @Override
    public RDD<Row> buildScan() {
        return dataset.rdd();
    }
}
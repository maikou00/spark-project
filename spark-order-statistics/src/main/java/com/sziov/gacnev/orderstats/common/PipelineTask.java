package com.sziov.gacnev.orderstats.common;

import org.apache.spark.sql.SparkSession;

@FunctionalInterface
public interface PipelineTask {
    void run(SparkSession spark, String dt) throws Exception;
}

package com.sziov.gacnev.datasource.hive;

import com.sziov.gacnev.AbstractSparkTest;
import com.sziov.gacnev.datasource.core.ReadOptions;
import com.sziov.gacnev.datasource.core.WriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("HiveSourceSink 真实场景测试")
class HiveSourceSinkTest extends AbstractSparkTest {

    private static Dataset<Row> sampleDf() {
        return spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice"),
                RowFactory.create(2, "Bob")
        ), new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        }));
    }

    private static String uniqueTable() {
        return "tbl_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    @Test
    @DisplayName("HiveSink_write_append_数据被写入表")
    void hiveSink_write_append_writesDataToTable() {
        String tbl = uniqueTable();
        sampleDf().write().mode("overwrite").saveAsTable(tbl);
        Map<String, String> extraOptions = new HashMap<>();
        extraOptions.put("database", "default");
        HiveConfig config = new HiveConfig();
        config.setExtraOptions(extraOptions);
        WriteOptions opts = WriteOptions.builder().resource(tbl).writeMode("append").build();
        new HiveSink(config).write(sampleDf(), opts);
        assertThat(spark.table(tbl).count()).isEqualTo(4);
    }

    @Test
    @DisplayName("HiveSource_read_带database_读取成功")
    void hiveSource_read_withDatabase_readsCorrectly() {
        String tbl = uniqueTable();
        sampleDf().write().mode("overwrite").saveAsTable(tbl);
        HiveConfig config = new HiveConfig();
        ReadOptions opts = ReadOptions.builder().resource(tbl).database("default").build();
        Dataset<Row> result = new HiveSource(config).read(spark, opts);
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("HiveSource_read_无database_读取成功")
    void hiveSource_read_noDatabase_readsCorrectly() {
        String tbl = uniqueTable();
        sampleDf().write().mode("overwrite").saveAsTable(tbl);
        HiveConfig config = new HiveConfig();
        ReadOptions opts = ReadOptions.builder().resource(tbl).build();
        Dataset<Row> result = new HiveSource(config).read(spark, opts);
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("HiveSink_write_overwrite_覆盖分区写入")
    void hiveSink_write_overwrite_writesWithPartition() {
        String tbl = uniqueTable();
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice"),
                RowFactory.create(2, "Bob")
        ), new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        }));
        df.withColumn("dt", org.apache.spark.sql.functions.lit("2026-06-09"))
                .write().mode("overwrite").partitionBy("dt").saveAsTable(tbl);
        Map<String, String> extraOptions = new HashMap<>();
        extraOptions.put("database", "default");
        extraOptions.put("partitionKey", "dt");
        HiveConfig config = new HiveConfig();
        config.setExtraOptions(extraOptions);
        WriteOptions opts = WriteOptions.builder().resource(tbl).writeMode("overwrite").build();
        new HiveSink(config).write(df, opts);
        assertThat(spark.table(tbl).count()).isGreaterThanOrEqualTo(2);
    }
}

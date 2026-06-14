package com.sziov.gacnev.utils.meta;
import com.sziov.gacnev.utils.etl.EtlUtils;

import com.sziov.gacnev.AbstractSparkTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link PartitionUtils} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("PartitionUtils 分区工具测试")
class PartitionUtilsTest extends AbstractSparkTest {

    @Test
    @DisplayName("repartition_指定分区数_返回重分区DataFrame")
    void repartition_specifiedPartitions_returnsRepartitionedDf() {
        Dataset<Row> df = spark.range(10).toDF();
        Dataset<Row> repartitioned = EtlUtils.repartition(df, 2);
        assertThat(repartitioned.rdd().getNumPartitions()).isEqualTo(2);
    }

    @Test
    @DisplayName("repartition_按列分区_返回重分区DataFrame")
    void repartition_byColumns_returnsRepartitionedDf() {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("dept", DataTypes.StringType, true)
        });
        Dataset<Row> df = spark.createDataFrame(java.util.Arrays.asList(
                RowFactory.create(1, "A"), RowFactory.create(2, "B")
        ), schema);
        Dataset<Row> repartitioned = EtlUtils.repartition(df, 1, "dept");
        assertThat(repartitioned).isNotNull();
    }

    @Test
    @DisplayName("coalesce_减少分区_返回合并后DataFrame")
    void coalesce_reducePartitions_returnsCoalescedDf() {
        Dataset<Row> df = spark.range(10).toDF().repartition(4);
        Dataset<Row> coalesced = EtlUtils.coalesce(df, 1);
        assertThat(coalesced.rdd().getNumPartitions()).isEqualTo(1);
    }

    @Test
    @DisplayName("getCurrentPartitionNum_DataFrame_返回分区数")
    void getCurrentPartitionNum_dataFrame_returnsPartitionCount() {
        Dataset<Row> df = spark.range(10).toDF().repartition(3);
        assertThat(EtlUtils.getCurrentPartitionNum(df)).isEqualTo(3);
    }
}

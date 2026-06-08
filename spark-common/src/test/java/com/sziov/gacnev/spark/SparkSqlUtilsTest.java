package com.sziov.gacnev.spark;

import com.sziov.gacnev.AbstractSparkTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SparkSqlUtils} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("SparkSqlUtils SQL工具测试")
class SparkSqlUtilsTest extends AbstractSparkTest {

    @Test
    @DisplayName("executeQuery_有效SQL_返回非空Dataset")
    void executeQuery_validSql_returnsDataset() {
        Dataset<Row> df = SparkSqlUtils.executeQuery(spark, "SELECT 1 AS col");
        assertThat(df).isNotNull();
        assertThat(df.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("executeQuery_错误SQL_抛出RuntimeException")
    void executeQuery_invalidSql_throwsRuntimeException() {
        assertThatThrownBy(() -> SparkSqlUtils.executeQuery(spark, "INVALID SQL SYNTAX"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("executeUpdate_有效DDL_执行成功")
    void executeUpdate_validDdl_executesSuccessfully() {
        SparkSqlUtils.executeUpdate(spark, "CREATE TABLE IF NOT EXISTS test_tbl (id INT) USING parquet");
        assertThat(spark.catalog().tableExists("test_tbl")).isTrue();
        spark.sql("DROP TABLE IF EXISTS test_tbl");
    }

    @Test
    @DisplayName("createTempView_Dataset_视图创建成功")
    void createTempView_dataset_createsView() {
        Dataset<Row> df = SparkSqlUtils.executeQuery(spark, "SELECT 1 AS id");
        SparkSqlUtils.createTempView(df, "test_view");
        assertThat(spark.catalog().tableExists("test_view")).isTrue();
        SparkSqlUtils.dropTempView(spark, "test_view");
    }

    @Test
    @DisplayName("createDatabase_数据库名_创建成功")
    void createDatabase_dbName_createsSuccessfully() {
        SparkSqlUtils.createDatabase(spark, "test_db");
        assertThat(spark.catalog().databaseExists("test_db")).isTrue();
        SparkSqlUtils.dropDatabase(spark, "test_db", true);
    }

    @Test
    @DisplayName("show_DataFrame_不抛异常")
    void show_dataFrame_noException() {
        Dataset<Row> df = SparkSqlUtils.executeQuery(spark, "SELECT 1 AS id");
        SparkSqlUtils.show(df);
    }

    @Test
    @DisplayName("getTableSchema_有效表名_返回StructType")
    void getTableSchema_validTable_returnsSchema() {
        SparkSqlUtils.executeUpdate(spark, "CREATE TABLE IF NOT EXISTS t1 (id INT, name STRING) USING parquet");
        StructType schema = SparkSqlUtils.getTableSchema(spark, "t1");
        assertThat(schema.fieldNames()).contains("id", "name");
        spark.sql("DROP TABLE IF EXISTS t1");
    }
}

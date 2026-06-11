package com.sziov.gacnev.datasource;

import com.sziov.gacnev.AbstractSparkTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MySQL 集成测试")
@Slf4j
class DataSourceMysqlTest extends AbstractSparkTest {

    private static final String JDBC_URL =
            "jdbc:mysql://localhost:3307/spark_test?rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true";
    private static final String USER = "root";
    private static final String PASSWORD = "root123";
    private static final String DB = "spark_test";

    @BeforeAll
    static void initConfig() {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("datasource.mysql.url", JDBC_URL);
        props.setProperty("datasource.mysql.username", USER);
        props.setProperty("datasource.mysql.password", PASSWORD);
        DataSources.init(props);
    }

    @AfterAll
    static void resetConfig() {
        DataSources.init(null);
    }

    private static String tbl(String name) {
        return DB + "." + name;
    }

    private static Dataset<Row> sampleDf() {
        return spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice", 100.50),
                RowFactory.create(2, "Bob", 200.00)
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true)
        }));
    }

    private static Dataset<Row> sampleDfWithDt() {
        return spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice", 100.50, "2026-06-09"),
                RowFactory.create(2, "Bob", 200.00, "2026-06-09"),
                RowFactory.create(3, "Cathy", 300.75, "2026-06-10"),
                RowFactory.create(4, "David", 400.00, "2026-06-10")
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("dt", DataTypes.StringType, true)
        }));
    }

    @Test
    @DisplayName("MySQL_read_整表读取_返回全部数据")
    void read_fullTable() {
        String t = tbl("mysql_read_test");
        DataSources.mysql().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.mysql().read(spark, t);
        assertThat(result.count()).isEqualTo(2);
        assertThat(result.columns()).containsExactly("id", "name", "amount");

        dropTable(t);
    }

    @Test
    @DisplayName("MySQL_read_谓词分区读取_数据完整")
    void read_predicates() {
        String t = tbl("mysql_predicates_test");
        DataSources.mysql().write(sampleDfWithDt(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.mysql()
                .option(o -> o.setPredicates(Arrays.asList(
                        t + ".dt='2026-06-09'",
                        t + ".dt='2026-06-10'")))
                .read(spark, t);
        assertThat(result.count()).isEqualTo(4);

        dropTable(t);
    }

    @Test
    @DisplayName("MySQL_read_分区读取_数据完整")
    void read_partitioned() {
        String t = tbl("mysql_partitioned_test");
        DataSources.mysql().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.mysql()
                .option(o -> o.setPartitionColumn("id").setLowerBound(0L).setUpperBound(10L).setNumPartitions(4))
                .read(spark, t);
        assertThat(result.count()).isEqualTo(2);

        dropTable(t);
    }

    @Test
    @DisplayName("MySQL_read_自定义SQL_返回查询结果")
    void read_customQuery() {
        String t = tbl("mysql_query_test");
        DataSources.mysql().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.mysql()
                .option(o -> o.setQuery("SELECT id, name FROM " + t + " WHERE id = 1"))
                .read(spark, null);
        assertThat(result.count()).isEqualTo(1);

        dropTable(t);
    }

    @Test
    @DisplayName("MySQL_表名无数据库前缀_抛WarehouseException")
    void read_missingDatabase() {
        try {
            DataSources.mysql().read(spark, "bare_table");
        } catch (com.sziov.gacnev.common.WarehouseException e) {
            log.error(e.getMessage());
            return;
        }
        org.assertj.core.api.Assertions.fail("应该抛出 WarehouseException");
    }

    @Test
    @DisplayName("MySQL_write_Append_数据追加成功")
    void write_append() {
        String t = tbl("mysql_write_append_test");
        DataSources.mysql().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        DataSources.mysql()
                .option(o -> o.setWriteMode(SaveMode.Append))
                .write(sampleDf(), t);

        Dataset<Row> result = DataSources.mysql().read(spark, t);
        assertThat(result.count()).isEqualTo(4);

        dropTable(t);
    }

    @Test
    @DisplayName("MySQL_write_Overwrite_覆盖写入成功")
    void write_overwrite() {
        String t = tbl("mysql_write_overwrite_test");
        DataSources.mysql().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> newDf = spark.createDataFrame(Arrays.asList(
                RowFactory.create(3, "Cathy", 300.75)
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true)
        }));

        DataSources.mysql()
                .option(o -> o.setWriteMode(SaveMode.Overwrite))
                .write(newDf, t);

        Dataset<Row> result = DataSources.mysql().read(spark, t);
        assertThat(result.count()).isEqualTo(1);
        assertThat((String) result.select("name").first().get(0)).isEqualTo("Cathy");

        dropTable(t);
    }

    @Test
    @DisplayName("MySQL_upsert_新增不存在的数据_插入成功")
    void upsert_insertNew() {
        String t = tbl("mysql_upsert_insert_test");
        dropTable(t);
        createTableWithPk(t);
        DataSources.mysql().write(sampleDf(), o -> o.setWriteMode(SaveMode.Append).setResource(t));

        Dataset<Row> newDf = spark.createDataFrame(Arrays.asList(
                RowFactory.create(3, "Cathy", 300.75)
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true)
        }));

        DataSources.mysql()
                .option(o -> o.setUpsertKeys(Collections.singletonList("id")))
                .upsert(newDf, t);

        Dataset<Row> result = DataSources.mysql().read(spark, t);
        assertThat(result.count()).isEqualTo(3);

        dropTable(t);
    }

    @Test
    @DisplayName("MySQL_upsert_主键冲突时更新_数据正确覆盖")
    void upsert_updateExisting() {
        String t = tbl("mysql_upsert_update_test");
        dropTable(t);
        createTableWithPk(t);
        DataSources.mysql().write(sampleDf(), o -> o.setWriteMode(SaveMode.Append).setResource(t));

        Dataset<Row> updateDf = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice_new", 150.00),
                RowFactory.create(3, "Cathy", 300.75)
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true)
        }));

        DataSources.mysql()
                .option(o -> o.setUpsertKeys(Collections.singletonList("id")))
                .upsert(updateDf, t);

        Dataset<Row> result = DataSources.mysql().read(spark, t);
        assertThat(result.count()).isEqualTo(3);
        Row row1 = result.filter("id = 1").first();
        assertThat(row1.getString(1)).isEqualTo("Alice_new");
        assertThat(row1.getDouble(2)).isEqualTo(150.00);

        dropTable(t);
    }

    @Test
    @DisplayName("MySQL_execute_执行DELETE_数据被删除")
    void execute_delete() {
        String t = tbl("mysql_execute_delete_test");
        DataSources.mysql().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        DataSources.mysql()
                .option(o -> o.setQuery("DELETE FROM " + t + " WHERE id = 1"))
                .execute();

        Dataset<Row> result = DataSources.mysql().read(spark, t);
        assertThat(result.count()).isEqualTo(1);
        assertThat((Integer) result.first().get(0)).isEqualTo(2);

        dropTable(t);
    }

    @Test
    @DisplayName("MySQL_execute_执行UPDATE_数据已更新")
    void execute_update() {
        String t = tbl("mysql_execute_update_test");
        DataSources.mysql().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        DataSources.mysql()
                .option(o -> o.setQuery("UPDATE " + t + " SET amount = 999 WHERE id = 2"))
                .execute();

        Dataset<Row> result = DataSources.mysql().read(spark, t);
        Row row = result.filter("id = 2").first();
        assertThat(row.getDouble(2)).isEqualTo(999.00);

        dropTable(t);
    }

    private static void createTableWithPk(String tbl) {
        java.sql.Connection conn = null;
        java.sql.Statement stmt = null;
        try {
            conn = java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
            stmt = conn.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS " + tbl
                    + " (id BIGINT PRIMARY KEY, name VARCHAR(100), amount DOUBLE)");
        } catch (Exception ignored) {
        } finally {
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }
    }

    private static void dropTable(String tbl) {
        java.sql.Connection conn = null;
        java.sql.Statement stmt = null;
        try {
            conn = java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
            stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS " + tbl);
        } catch (Exception ignored) {
        } finally {
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }
    }
}

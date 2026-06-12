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

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ClickHouse 集成测试")
@Slf4j
class DataSourceClickHouseTest extends AbstractSparkTest {

    private static final String JDBC_URL = "jdbc:clickhouse://localhost:8123/default";
    private static final String USER = "default";
    private static final String PASSWORD = "ck123";

    @BeforeAll
    static void initConfig() {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("datasource.ck.hosts", JDBC_URL);
        props.setProperty("datasource.ck.username", USER);
        props.setProperty("datasource.ck.password", PASSWORD);
        DataSources.init(props);
    }

    @AfterAll
    static void resetConfig() {
        DataSources.init(null);
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

    @Test
    @DisplayName("CK_read_整表读取_返回全部数据")
    void readFullTable() {
        String t = "ck_read_test";
        createTable(t);
        DataSources.clickhouse().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.clickhouse().read(spark, t);
        assertThat(result.count()).isEqualTo(2);
        assertThat(result.columns()).containsExactly("id", "name", "amount");

        dropTable(t);
    }

    @Test
    @DisplayName("CK_read_自定义SQL_返回查询结果")
    void readCustomQuery() {
        String t = "ck_query_test";
        createTable(t);
        DataSources.clickhouse().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.clickhouse()
                .option(o -> o.setQuery("SELECT id, name FROM default." + t + " WHERE id = 1"))
                .read(spark, null);
        assertThat(result.count()).isEqualTo(1);

        dropTable(t);
    }

    @Test
    @DisplayName("CK_write_Append追加_数据累加")
    void writeAppend() {
        String t = "ck_append_test";
        createTable(t);
        DataSources.clickhouse().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        DataSources.clickhouse().write(sampleDf(), o -> o.setWriteMode(SaveMode.Append).setResource(t));

        Dataset<Row> result = DataSources.clickhouse().read(spark, t);
        assertThat(result.count()).isEqualTo(4);

        dropTable(t);
    }

    @Test
    @DisplayName("CK_write_Overwrite覆盖_旧数据被清空")
    void writeOverwrite() {
        String t = "ck_overwrite_test";
        createTable(t);
        DataSources.clickhouse().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> newDf = spark.createDataFrame(Arrays.asList(
                RowFactory.create(3, "Cathy", 300.75)
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true)
        }));
        DataSources.clickhouse().write(newDf, o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.clickhouse().read(spark, t);
        assertThat(result.count()).isEqualTo(1);
        assertThat((Integer) result.first().get(0)).isEqualTo(3);

        dropTable(t);
    }

    private static void createTable(String tbl) {
        java.sql.Connection conn = null;
        java.sql.Statement stmt = null;
        try {
            conn = java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
            stmt = conn.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS default." + tbl
                    + " (id Int32, name String, amount Float64) ENGINE = MergeTree() ORDER BY id");
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
            stmt.execute("DROP TABLE IF EXISTS default." + tbl);
        } catch (Exception ignored) {
        } finally {
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }
    }
}

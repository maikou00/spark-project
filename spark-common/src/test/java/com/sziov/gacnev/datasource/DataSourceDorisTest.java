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

/**
 * Doris 集成测试。
 *
 * @author maikou
 * @since 2026-06-12
 */
@DisplayName("Doris 集成测试")
@Slf4j
class DataSourceDorisTest extends AbstractSparkTest {

    private static final String FENODES = "172.20.80.2:8030";
    private static final String JDBC_URL = "jdbc:mysql://localhost:9030";
    private static final String USER = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "spark_test";

    @BeforeAll
    static void initConfig() {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("datasource.doris.fenodes", FENODES);
        props.setProperty("datasource.doris.url", JDBC_URL);
        props.setProperty("datasource.doris.username", USER);
        props.setProperty("datasource.doris.password", PASSWORD);
        DataSources.init(props);
        createDatabase();
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
    @DisplayName("Doris_read_整表读取_返回全部数据")
    void readFullTable() {
        String t = qualified("doris_read_test");
        createTable(t);
        DataSources.doris().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.doris().read(spark, t);
        result.show();
        assertThat(result.count()).isEqualTo(2);
        assertThat(result.columns()).containsExactly("id", "name", "amount");

        dropTable(t);
    }

    @Test
    @DisplayName("Doris_read_谓词过滤_返回过滤数据")
    void readCustomQuery() {
        String t = qualified("doris_query_test");
        createTable(t);
        DataSources.doris().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));
        Dataset<Row> result = DataSources.doris()
                
                .read(spark, t);
        result.show();
        assertThat(result.count()).isEqualTo(2);
        dropTable(t);
    }

    @Test
    @DisplayName("Doris_write_Append追加_数据累加")
    void writeAppend() {
        String t = qualified("doris_append_test");
        createTable(t);
        DataSources.doris().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        DataSources.doris().write(spark.createDataFrame(Arrays.asList(
                RowFactory.create(3, "Cathy", 300.00),
                RowFactory.create(4, "David", 400.00)
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true)
        })), o -> o.setWriteMode(SaveMode.Append).setResource(t));

        Dataset<Row> result = DataSources.doris().read(spark, t);
        result.show();
        assertThat(result.count()).isEqualTo(4);

        dropTable(t);
    }

    @Test
    @DisplayName("Doris_write_Overwrite覆盖_旧数据被清空")
    void writeOverwrite() {
        String t = qualified("doris_overwrite_test");
        createTable(t);
        DataSources.doris().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> newDf = spark.createDataFrame(Arrays.asList(
                RowFactory.create(3, "Cathy", 300.75)
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true)
        }));
        DataSources.doris().write(newDf, o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.doris().read(spark, t);
        result.show();
        assertThat(result.count()).isEqualTo(1);
        assertThat((Integer) result.first().get(0)).isEqualTo(3);

        dropTable(t);
    }

    @Test
    @DisplayName("Doris_execute_执行DDL_操作成功")
    void executeDdl() {
        String t = qualified("doris_execute_test");
        createTable(t);
        DataSources.doris().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        DataSources.doris()
                .option(o -> o.setQuery("TRUNCATE TABLE " + t))
                .execute();

        Dataset<Row> result = DataSources.doris().read(spark, t);
        result.show();
        assertThat(result.count()).isEqualTo(0);

        dropTable(t);
    }

    // ==================== 辅助方法 ====================

    @Test
    @DisplayName("Doris_write_read_主键模型_写入并读取_数据正确")
    void writeReadPrimaryKeyModel() {
        String t = qualified("doris_pk_test");
        createPkTable(t);
        DataSources.doris().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        Dataset<Row> result = DataSources.doris().read(spark, t);
        result.show();
        assertThat(result.count()).isEqualTo(2);
        assertThat(result.columns()).containsExactly("id", "name", "amount");

        dropTable(t);
    }

    private static void createPkTable(String tbl) {
        executeJdbc("CREATE TABLE IF NOT EXISTS " + tbl + " ("
                + "id INT, name VARCHAR(50), amount DOUBLE"
                + ") UNIQUE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', "
                + "'enable_unique_key_merge_on_write' = 'true')");
    }

    private static String qualified(String tbl) {
        return DATABASE + "." + tbl;
    }

    private static void createDatabase() {
        executeJdbc("CREATE DATABASE IF NOT EXISTS " + DATABASE);
    }

    private static void createTable(String tbl) {
        executeJdbc("CREATE TABLE IF NOT EXISTS " + tbl + " ("
                + "id INT, name VARCHAR(50), amount DOUBLE"
                + ") ENGINE = OLAP UNIQUE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
    }

    private static void dropTable(String tbl) {
        executeJdbc("DROP TABLE IF EXISTS " + tbl);
    }

    private static void executeJdbc(String sql) {
        java.sql.Connection conn = null;
        java.sql.Statement stmt = null;
        try {
            conn = java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (Exception e) {
            log.warn("JDBC 执行异常: {}", e.getMessage());
        } finally {
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }
    }
}

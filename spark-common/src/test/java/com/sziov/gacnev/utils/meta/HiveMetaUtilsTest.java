package com.sziov.gacnev.utils.meta;

import com.sziov.gacnev.AbstractSparkTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link HiveMetaUtils} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("HiveMetaUtils 元数据工具测试")
class HiveMetaUtilsTest extends AbstractSparkTest {

    private static final String TEST_DB = "test_meta_db";
    private static final String TEST_TABLE = "test_meta_tbl";

    @BeforeEach
    void setUp() {
        spark.sql("CREATE DATABASE IF NOT EXISTS " + TEST_DB);
        spark.sql("USE " + TEST_DB);
        spark.sql("CREATE TABLE IF NOT EXISTS " + TEST_TABLE + " (id INT, name STRING) USING parquet");
    }

    @Test
    @DisplayName("getDatabaseList_返回非空列表")
    void getDatabaseList_returnsNonEmptyList() {
        List<String> dbs = HiveMetaUtils.getDatabaseList(spark);
        assertThat(dbs).isNotEmpty();
        assertThat(dbs).contains(TEST_DB);
    }

    @Test
    @DisplayName("getTableList_有效库名_返回表列表")
    void getTableList_validDb_returnsTableList() {
        List<String> tables = HiveMetaUtils.getTableList(spark, TEST_DB);
        assertThat(tables).contains(TEST_TABLE);
    }

    @Test
    @DisplayName("databaseExists_存在库_返回true")
    void databaseExists_existingDb_returnsTrue() {
        assertThat(HiveMetaUtils.databaseExists(spark, TEST_DB)).isTrue();
    }

    @Test
    @DisplayName("databaseExists_不存在库_返回false")
    void databaseExists_nonExistingDb_returnsFalse() {
        assertThat(HiveMetaUtils.databaseExists(spark, "non_existent_db")).isFalse();
    }

    @Test
    @DisplayName("tableExists_存在表_返回true")
    void tableExists_existingTable_returnsTrue() {
        assertThat(HiveMetaUtils.tableExists(spark, TEST_DB + "." + TEST_TABLE)).isTrue();
    }

    @Test
    @DisplayName("tableExists_不存在表_返回false")
    void tableExists_nonExistingTable_returnsFalse() {
        assertThat(HiveMetaUtils.tableExists(spark, "non_existent_tbl")).isFalse();
    }

    @Test
    @DisplayName("getTableColumns_有效表名_返回列信息")
    void getTableColumns_validTable_returnsColumns() {
        assertThat(HiveMetaUtils.getTableColumns(spark, TEST_DB + "." + TEST_TABLE)).isNotNull();
    }
}

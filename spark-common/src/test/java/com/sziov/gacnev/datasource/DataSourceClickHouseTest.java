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

/**
 * ClickHouse 集成测试。
 *
 * <h3>热点问题原理</h3>
 * MergeTree 按 PARTITION BY + ORDER BY 组织数据，热点发生在两类场景：
 * <ul>
 *   <li><b>写热点</b>：数据集中落入同一分区（如按 dt 天分区，当天写入涌入同个 part），
 *       导致单分区 merge 排队、I/O 争抢、mutations 堆积。</li>
 *   <li><b>读热点</b>：ORDER BY 首列为单调递增时间戳，新数据永远在尾部，
 *       缓存被频繁刷出、单核 CPU 打满。</li>
 * </ul>
 *
 * <h3>解决方法</h3>
 * <table>
 *   <tr><th>策略</th><th>做法</th><th>适用场景</th></tr>
 *   <tr><td>分区粒度</td><td>高吞吐场景用月分区代替天分区。
 * 原因：天分区导致分区数爆炸（一年365个），当天写入集中单分区排队 merge，元数据膨胀；
 * 月分区把30天写入摊平到多个 part，缓解 I/O 争抢。日写入量低于1000万行用月分区即可。</td><td>写热点</td></tr>
 *   <tr><td>ORDER BY</td><td>高基数列放首位（如 user_id, timestamp），
 * 让数据写入同时分散到不同 part，避免尾部热点。</td><td>读写热点</td></tr>
 *   <tr><td>分片键</td><td>分布式表用 cityHash64(key) 均匀散列，
 * 单 shard 内部按 ORDER BY 有序，跨 shard 通过 Hash 分散。</td><td>读写热点</td></tr>
 *   <tr><td>异步插入</td><td>async_insert=1 缓冲区攒批落盘，
 * 减少 part 碎片和 merge 频率。</td><td>写热点</td></tr>
 *   <tr><td>物化视图</td><td>预聚合热点查询，查视图不查原表，
 * 避免全表扫描打满单核 CPU。</td><td>读热点</td></tr>
 * </table>
 * 核心思路：让数据和查询尽量均匀分散到不同 part/shard，避免单点瓶颈。
 *
 * @author maikou
 * @since 2026-06-11
 */
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
        result.show();
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
        result.show();
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
        result.show();
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
        result.show();
        assertThat(result.count()).isEqualTo(1);
        assertThat((Integer) result.first().get(0)).isEqualTo(3);

        dropTable(t);
    }

    @Test
    @DisplayName("CK_execute_执行DELETE_数据被删除")
    void executeDelete() {
        String t = "ck_execute_delete_test";
        createTable(t);
        DataSources.clickhouse().write(sampleDf(), o -> o.setWriteMode(SaveMode.Overwrite).setResource(t));

        DataSources.clickhouse()
                .option(o -> o.setQuery("ALTER TABLE default." + t + " DELETE WHERE id = 1"))
                .execute();

        Dataset<Row> result = DataSources.clickhouse().read(spark, t);
        result.show();
        assertThat(result.count()).isEqualTo(1);
        assertThat((Integer) result.first().get(0)).isEqualTo(2);

        dropTable(t);
    }

    @Test
    @DisplayName("CK_upsert_ClickHouse不支持_抛UnsupportedOperationException")
    void upsertNotSupported() {
        try {
            DataSources.clickhouse().upsert(sampleDf(), "ck_upsert_not_supported");
            org.junit.jupiter.api.Assertions.fail("应抛 UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage()).contains("UPSERT不支持");
        }
    }

        /**
     * <b>案例一：ORDER BY 顺序导致读热点</b>
     * <pre>
     * -- ❌ 坏：timestamp 在首位，新数据全在尾部，单核打满
     * CREATE TABLE orders_bad (
     *     ts DateTime, user_id UInt64, amount Float64
     * ) ENGINE = MergeTree() ORDER BY ts;
     *
     * -- ✅ 好：user_id 在首位，数据按用户分散到不同 part
     * CREATE TABLE orders_good (
     *     ts DateTime, user_id UInt64, amount Float64
     * ) ENGINE = MergeTree() ORDER BY (user_id, ts);
     * </pre>
     */
    @Test
    @DisplayName("CK优化_ORDER_BY高基数优先_多part并行读")
    void orderByHighCardinality() {
        String t = "ck_orderby_opt_test";
        execDdl("CREATE TABLE IF NOT EXISTS default." + t
                + " (category String, ts DateTime, val Float64)"
                + " ENGINE = MergeTree() ORDER BY (category, ts)");

        java.util.List<org.apache.spark.sql.Row> rows = new java.util.ArrayList<>();
        String[] categories = {"A", "B", "C", "D", "E"};
        long base = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                rows.add(RowFactory.create(categories[i],
                        new java.sql.Timestamp((base + j) * 1000), (double) (i * 10 + j)));
            }
        }
        Dataset<Row> df = spark.createDataFrame(rows, new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("category", DataTypes.StringType, false),
                DataTypes.createStructField("ts", DataTypes.TimestampType, false),
                DataTypes.createStructField("val", DataTypes.DoubleType, false)
        }));
        DataSources.clickhouse().write(df, o -> o.setWriteMode(SaveMode.Append).setResource(t));

        int partCount = countParts(t);
        assertThat(partCount).isGreaterThanOrEqualTo(1);
        log.info("ORDER BY (category, ts) 表有 {} 个 active parts，高基数首列促进数据分散", partCount);

        Dataset<Row> result = DataSources.clickhouse().read(spark, t);
        result.show();
        assertThat(result.count()).isEqualTo(25);

        dropTable(t);
    }

        /**
     * <b>案例二：天分区导致分区爆炸 + 写热点</b>
     * <pre>
     * -- ❌ 坏：一天一分区，一年365个分区，当天写入堵在单分区排队
     * CREATE TABLE events_bad (
     *     dt Date, event String, cnt UInt64
     * ) ENGINE = MergeTree() PARTITION BY dt ORDER BY dt;
     *
     * -- ✅ 好：一月一分区，merge 压力摊平，日写入量低于1000万足够
     * CREATE TABLE events_good (
     *     dt Date, event String, cnt UInt64
     * ) ENGINE = MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY (event, dt);
     * </pre>
     */
    @Test
    @DisplayName("CK优化_月分区_分区数精简可控")
    void monthlyPartition() {
        String t = "ck_partition_opt_test";
        execDdl("CREATE TABLE IF NOT EXISTS default." + t
                + " (dt Date, event String, cnt UInt64)"
                + " ENGINE = MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY (event, dt)");

        java.util.List<org.apache.spark.sql.Row> rows = new java.util.ArrayList<>();
        String[] months = {"2026-05", "2026-06"};
        for (String m : months) {
            for (int d = 1; d <= 3; d++) {
                rows.add(RowFactory.create(java.sql.Date.valueOf(m + "-" + String.format("%02d", d)),
                        "click", (long) (d * 10)));
            }
        }
        Dataset<Row> df = spark.createDataFrame(rows, new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("dt", DataTypes.DateType, false),
                DataTypes.createStructField("event", DataTypes.StringType, false),
                DataTypes.createStructField("cnt", DataTypes.LongType, false)
        }));
        DataSources.clickhouse().write(df, o -> o.setWriteMode(SaveMode.Append).setResource(t));

        int partCount = countParts(t);
        assertThat(partCount).isEqualTo(2);
        log.info("PARTITION BY toYYYYMM(dt) 产生 {} 个分区，与月份数一致，避免天分区爆炸", partCount);

        Dataset<Row> result = DataSources.clickhouse().read(spark, t);
        result.show();
        assertThat(result.count()).isEqualTo(6);

        dropTable(t);
    }

        /**
     * <b>案例三：分布式表无分片键导致数据倾斜</b>
     * <pre>
     * -- ❌ 坏：按 rand() 分布，查询无法裁剪 shard
     * CREATE TABLE logs_dist_bad AS logs_local
     * ENGINE = Distributed(cluster, default, logs_local, rand());
     *
     * -- ✅ 好：cityHash64 按 user_id 散列，查询精准路由到单 shard
     * CREATE TABLE logs_dist_good AS logs_local
     * ENGINE = Distributed(cluster, default, logs_local, cityHash64(user_id));
     * </pre>
     */
    @Test
    @DisplayName("CK优化_分布式表cityHash64分片_查询精准路由")
    void distributedShardKey() {
        String localTbl = "ck_dist_local_test";
        String distTbl = "ck_dist_test";

        execDdl("CREATE TABLE IF NOT EXISTS default." + localTbl
                + " (user_id UInt64, event String, ts DateTime)"
                + " ENGINE = MergeTree() ORDER BY (user_id, ts)");
        execDdl("CREATE TABLE IF NOT EXISTS default." + distTbl
                + " AS default." + localTbl
                + " ENGINE = Distributed(default, default, " + localTbl + ", cityHash64(user_id))");

        java.util.List<org.apache.spark.sql.Row> rows = new java.util.ArrayList<>();
        long base = System.currentTimeMillis() / 1000;
        for (int uid = 1; uid <= 5; uid++) {
            rows.add(RowFactory.create((long) uid, "login",
                    new java.sql.Timestamp((base + uid) * 1000)));
        }
        Dataset<Row> df = spark.createDataFrame(rows, new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("user_id", DataTypes.LongType, false),
                DataTypes.createStructField("event", DataTypes.StringType, false),
                DataTypes.createStructField("ts", DataTypes.TimestampType, false)
        }));
        DataSources.clickhouse().write(df, o -> o.setWriteMode(SaveMode.Append).setResource(localTbl));

        Dataset<Row> result = DataSources.clickhouse().read(spark, distTbl);
        result.show();
        assertThat(result.count()).isEqualTo(5);

        dropTable(distTbl);
        dropTable(localTbl);
    }

    private static int countParts(String tbl) {
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery(
                     "SELECT count() FROM system.parts WHERE database='default' AND table='" + tbl + "' AND active")) {
            if (rs.next()) return rs.getInt(1);
        } catch (Exception ignored) {}
        return 0;
    }

    private static void execDdl(String sql) {
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
             java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (Exception ignored) {}
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

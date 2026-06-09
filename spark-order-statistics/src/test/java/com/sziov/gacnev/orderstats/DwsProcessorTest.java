package com.sziov.gacnev.orderstats;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DWS 层多粒度聚合测试。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("DwsProcessor 聚合测试")
class DwsProcessorTest {

    private static SparkSession spark;

    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName("dws-test")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.warehouse.dir",
                        System.getProperty("java.io.tmpdir") + "/spark-warehouse-dws-test")
                .config("spark.sql.catalogImplementation", "in-memory")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    @DisplayName("聚合_多维度汇总_每个维度独立聚合")
    void aggregate_multiDim_aggregatesPerDim() {
        List<Row> rows = Arrays.asList(
                createOrderRow("ORD001", "U0001", "P0001", "S0001", "110000", "100.00", "pay"),
                createOrderRow("ORD002", "U0001", "P0002", "S0001", "110000", "200.00", "sign"),
                createOrderRow("ORD003", "U0002", "P0001", "S0002", "310000", "300.00", "refund")
        );

        Dataset<Row> df = createTestDf(rows);
        df.createOrReplaceTempView("dwd_order_test");

        String aggregateSql = buildAggregateSql();
        Dataset<Row> result = spark.sql(aggregateSql);

        assertThat(result.count()).isGreaterThan(0);

        // 用户维度：U0001 有2条
        Dataset<Row> userU1 = result.filter("dim_type='user' AND dim_id='U0001'");
        assertThat(userU1.count()).isEqualTo(1);
        Row u1Row = userU1.first();
        assertThat(u1Row.getLong(u1Row.fieldIndex("order_count"))).isEqualTo(2);

        // 商品维度：P0001 有2条
        Dataset<Row> productP1 = result.filter("dim_type='product' AND dim_id='P0001'");
        assertThat(productP1.count()).isEqualTo(1);
        Row p1Row = productP1.first();
        assertThat(p1Row.getLong(p1Row.fieldIndex("order_count"))).isEqualTo(2);

        // 店铺维度：S0001 有2条
        Dataset<Row> storeS1 = result.filter("dim_type='store' AND dim_id='S0001'");
        Row s1Row = storeS1.first();
        assertThat(s1Row.getLong(s1Row.fieldIndex("order_count"))).isEqualTo(2);
    }

    @Test
    @DisplayName("聚合_支付和退单计数_正确统计")
    void aggregate_payAndRefund_countsCorrectly() {
        List<Row> rows = Arrays.asList(
                createOrderRow("ORD001", "U0001", "P0001", "S0001", "110000", "100.00", "pay"),
                createOrderRow("ORD002", "U0001", "P0001", "S0001", "110000", "200.00", "refund"),
                createOrderRow("ORD003", "U0001", "P0001", "S0001", "110000", "300.00", "create")
        );

        Dataset<Row> df = createTestDf(rows);
        df.createOrReplaceTempView("dwd_order_test");

        String aggregateSql = buildAggregateSql();
        Dataset<Row> result = spark.sql(aggregateSql);

        // 用户U0001维度
        Dataset<Row> userAgg = result.filter("dim_type='user' AND dim_id='U0001'");
        Row row = userAgg.first();
        assertThat(row.getLong(row.fieldIndex("order_count"))).isEqualTo(3);
        assertThat(row.getLong(row.fieldIndex("paid_count"))).isEqualTo(1);
        assertThat(row.getLong(row.fieldIndex("refund_count"))).isEqualTo(1);
    }

    private Row createOrderRow(String orderId, String userId, String productId,
                                String storeId, String regionId, String amount, String status) {
        return RowFactory.create(orderId, userId, productId, storeId, regionId,
                new BigDecimal(amount), status,
                "2026-06-09 10:00:00", "", "", "", "");
    }

    private Dataset<Row> createTestDf(List<Row> rows) {
        StructType schema = new StructType()
                .add("order_id", DataTypes.StringType)
                .add("user_id", DataTypes.StringType)
                .add("product_id", DataTypes.StringType)
                .add("store_id", DataTypes.StringType)
                .add("region_id", DataTypes.StringType)
                .add("order_amount", DataTypes.createDecimalType(18, 2))
                .add("order_status", DataTypes.StringType)
                .add("create_time", DataTypes.StringType)
                .add("pay_time", DataTypes.StringType)
                .add("ship_time", DataTypes.StringType)
                .add("sign_time", DataTypes.StringType)
                .add("refund_time", DataTypes.StringType);
        return spark.createDataFrame(rows, schema);
    }

    private String buildAggregateSql() {
        return "SELECT 'user' AS dim_type, user_id AS dim_id, "
                + "COUNT(1) AS order_count, "
                + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                + "SUM(CASE WHEN order_status IN ('pay','ship','sign') THEN 1 ELSE 0 END) AS paid_count, "
                + "SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) AS refund_count "
                + "FROM dwd_order_test "
                + "GROUP BY user_id "
                + "UNION ALL "
                + "SELECT 'product' AS dim_type, product_id AS dim_id, "
                + "COUNT(1) AS order_count, "
                + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                + "SUM(CASE WHEN order_status IN ('pay','ship','sign') THEN 1 ELSE 0 END) AS paid_count, "
                + "SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) AS refund_count "
                + "FROM dwd_order_test "
                + "GROUP BY product_id "
                + "UNION ALL "
                + "SELECT 'store' AS dim_type, store_id AS dim_id, "
                + "COUNT(1) AS order_count, "
                + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                + "SUM(CASE WHEN order_status IN ('pay','ship','sign') THEN 1 ELSE 0 END) AS paid_count, "
                + "SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) AS refund_count "
                + "FROM dwd_order_test "
                + "GROUP BY store_id "
                + "UNION ALL "
                + "SELECT 'region' AS dim_type, region_id AS dim_id, "
                + "COUNT(1) AS order_count, "
                + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                + "SUM(CASE WHEN order_status IN ('pay','ship','sign') THEN 1 ELSE 0 END) AS paid_count, "
                + "SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) AS refund_count "
                + "FROM dwd_order_test "
                + "GROUP BY region_id";
    }
}

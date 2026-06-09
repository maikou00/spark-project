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
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADS 层 KPI 计算测试。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("AdsProcessor KPI计算测试")
class AdsProcessorTest {

    private static SparkSession spark;

    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName("ads-test")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.warehouse.dir",
                        System.getProperty("java.io.tmpdir") + "/spark-warehouse-ads-test")
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
    @DisplayName("KPI计算_全部支付无退单_正确计算指标")
    void kpiCalc_allPaidNoRefund_correctMetrics() {
        List<Row> rows = Arrays.asList(
                createOrderRow("ORD001", "100.00", "pay"),
                createOrderRow("ORD002", "200.00", "ship"),
                createOrderRow("ORD003", "300.00", "sign"),
                createOrderRow("ORD004", "400.00", "create")
        );

        Dataset<Row> df = createTestDf(rows);
        df.createOrReplaceTempView("dwd_for_kpi_test");

        String kpiSql = buildKpiSql();
        Dataset<Row> result = spark.sql(kpiSql);
        Row kpi = result.first();

        assertThat(kpi.getLong(kpi.fieldIndex("total_orders"))).isEqualTo(4);
        assertThat(kpi.get(kpi.fieldIndex("total_gmv")).toString()).isEqualTo("1000.00");
        assertThat(kpi.get(kpi.fieldIndex("paid_orders")).toString()).isEqualTo("3");
        assertThat(kpi.getLong(kpi.fieldIndex("refund_orders"))).isEqualTo(0);
    }

    @Test
    @DisplayName("KPI计算_部分退单_退单率正确")
    void kpiCalc_withRefunds_refundRateCorrect() {
        List<Row> rows = Arrays.asList(
                createOrderRow("ORD001", "100.00", "pay"),
                createOrderRow("ORD002", "200.00", "refund"),
                createOrderRow("ORD003", "100.00", "sign"),
                createOrderRow("ORD004", "100.00", "pay"),
                createOrderRow("ORD005", "100.00", "refund")
        );

        Dataset<Row> df = createTestDf(rows);
        df.createOrReplaceTempView("dwd_for_kpi_test");

        String kpiSql = buildKpiSql();
        Dataset<Row> result = spark.sql(kpiSql);
        Row kpi = result.first();

        assertThat(kpi.getLong(kpi.fieldIndex("total_orders"))).isEqualTo(5);
        assertThat(kpi.get(kpi.fieldIndex("paid_orders")).toString()).isEqualTo("3");
        assertThat(kpi.getLong(kpi.fieldIndex("refund_orders"))).isEqualTo(2);
    }

    @Test
    @DisplayName("KPI计算_空数据_返回一行全零值")
    void kpiCalc_emptyData_returnsOneRow() {
        Dataset<Row> df = createTestDf(Collections.emptyList());
        df.createOrReplaceTempView("dwd_for_kpi_test");

        String kpiSql = buildKpiSql();
        Dataset<Row> result = spark.sql(kpiSql);

        // 空数据集聚合仍然返回一行（COUNT=0, SUM=NULL）
        assertThat(result.count()).isEqualTo(1);
        Row kpi = result.first();
        assertThat(kpi.getLong(kpi.fieldIndex("total_orders"))).isEqualTo(0);
        // SUM on empty set returns NULL, use get() with cast
        assertThat((Long) kpi.get(kpi.fieldIndex("refund_orders"))).isNull();
    }

    private Row createOrderRow(String orderId, String amount, String status) {
        return RowFactory.create(orderId, "U0001", "P0001", "S0001", "110000",
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

    private String buildKpiSql() {
        return "SELECT "
                + "COUNT(1) AS total_orders, "
                + "COALESCE(SUM(order_amount), 0) AS total_gmv, "
                + "CASE WHEN COUNT(1) > 0 "
                + "  THEN CAST(SUM(order_amount) / COUNT(1) AS DECIMAL(18,2)) "
                + "  ELSE 0 "
                + "END AS avg_order_amount, "
                + "SUM(CASE WHEN order_status IN ('pay','ship','sign') THEN 1 ELSE 0 END) AS paid_orders, "
                + "CASE WHEN COUNT(1) > 0 "
                + "  THEN CAST(SUM(CASE WHEN order_status IN ('pay','ship','sign') THEN 1 ELSE 0 END) * 1.0 "
                + "    / COUNT(1) AS DECIMAL(5,4)) "
                + "  ELSE 0 "
                + "END AS payment_rate, "
                + "SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) AS refund_orders, "
                + "CASE WHEN COUNT(1) > 0 "
                + "  THEN CAST(SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) * 1.0 "
                + "    / COUNT(1) AS DECIMAL(5,4)) "
                + "  ELSE 0 "
                + "END AS refund_rate "
                + "FROM dwd_for_kpi_test";
    }
}

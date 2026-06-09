package com.sziov.gacnev.orderstats;

import com.sziov.gacnev.orderstats.config.OrderStatsConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * DWD 层 JSON 解析与数据清洗测试。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("DwdProcessor 数据清洗测试")
class DwdProcessorTest {

    private static SparkSession spark;

    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName("dwd-test")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.warehouse.dir",
                        System.getProperty("java.io.tmpdir") + "/spark-warehouse-dwd-test")
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
    @DisplayName("fromJson_正常JSON_解析成功")
    void fromJson_validJson_parsesCorrectly() {
        String validJson = "{\"order_id\":\"ORD001\",\"user_id\":\"U0001\",\"product_id\":\"P0001\","
                + "\"store_id\":\"S0001\",\"region_id\":\"110000\",\"order_amount\":\"99.99\","
                + "\"order_status\":\"create\",\"create_time\":\"2026-06-09 10:00:00\","
                + "\"pay_time\":\"\",\"ship_time\":\"\",\"sign_time\":\"\",\"refund_time\":\"\"}";

        List<Row> rows = Arrays.asList(
                RowFactory.create("EVT001", "create", validJson, "2026-06-09 10:00:00")
        );

        Dataset<Row> df = spark.createDataFrame(rows, new org.apache.spark.sql.types.StructType()
                .add("event_id", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_type", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_data", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_time", org.apache.spark.sql.types.DataTypes.StringType));

        Dataset<Row> parsed = df.withColumn("parsed",
                from_json(col("event_data"), OrderStatsConfig.ORDER_EVENT_SCHEMA));

        // 正常JSON：parsed 不为 null，且 order_id 正确
        Dataset<Row> valid = parsed.filter(col("parsed").isNotNull());
        assertThat(valid.count()).isEqualTo(1);
        Row result = valid.selectExpr("parsed.order_id", "parsed.user_id").first();
        assertThat(result.getString(0)).isEqualTo("ORD001");
        assertThat(result.getString(1)).isEqualTo("U0001");
    }

    @Test
    @DisplayName("fromJson_无效JSON_内部字段全为null")
    void fromJson_brokenJson_fieldsAreNull() {
        String brokenJson = "DEFINITELY_NOT_JSON";

        List<Row> rows = Arrays.asList(
                RowFactory.create("EVT002", "create", brokenJson, "2026-06-09 10:00:00")
        );

        Dataset<Row> df = spark.createDataFrame(rows, new org.apache.spark.sql.types.StructType()
                .add("event_id", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_type", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_data", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_time", org.apache.spark.sql.types.DataTypes.StringType));

        Dataset<Row> parsed = df.withColumn("parsed",
                from_json(col("event_data"), OrderStatsConfig.ORDER_EVENT_SCHEMA));

        // Spark from_json 对无效JSON返回非null struct，但内部字段全为null
        // 通过检查 order_id（必填字段）是否为 null 来判断
        Row result = parsed.selectExpr("parsed.order_id", "parsed.user_id", "parsed.order_amount").first();
        assertThat(result.isNullAt(0)).isTrue();
        assertThat(result.isNullAt(1)).isTrue();
        assertThat(result.isNullAt(2)).isTrue();
    }

    @Test
    @DisplayName("fromJson_混合数据_通过必填字段非空过滤区分")
    void fromJson_mixedData_filterByRequiredFields() {
        String validJson = "{\"order_id\":\"ORD001\",\"user_id\":\"U0001\",\"product_id\":\"P0001\","
                + "\"store_id\":\"S0001\",\"region_id\":\"110000\",\"order_amount\":\"50.00\","
                + "\"order_status\":\"create\",\"create_time\":\"2026-06-09 10:00:00\","
                + "\"pay_time\":\"\",\"ship_time\":\"\",\"sign_time\":\"\",\"refund_time\":\"\"}";
        String brokenJson = "DEFINITELY_NOT_JSON";
        String emptyIdJson = "{\"order_id\":\"\",\"user_id\":\"U0003\",\"product_id\":\"P0001\","
                + "\"store_id\":\"S0001\",\"region_id\":\"110000\",\"order_amount\":\"30.00\","
                + "\"order_status\":\"create\",\"create_time\":\"2026-06-09 10:00:00\","
                + "\"pay_time\":\"\",\"ship_time\":\"\",\"sign_time\":\"\",\"refund_time\":\"\"}";

        List<Row> rows = Arrays.asList(
                RowFactory.create("EVT001", "create", validJson, "2026-06-09 10:00:00"),
                RowFactory.create("EVT002", "create", brokenJson, "2026-06-09 10:01:00"),
                RowFactory.create("EVT003", "create", emptyIdJson, "2026-06-09 10:02:00")
        );

        Dataset<Row> df = spark.createDataFrame(rows, new org.apache.spark.sql.types.StructType()
                .add("event_id", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_type", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_data", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_time", org.apache.spark.sql.types.DataTypes.StringType));

        // 解析并展开字段
        Dataset<Row> parsed = df.withColumn("parsed",
                from_json(col("event_data"), OrderStatsConfig.ORDER_EVENT_SCHEMA))
                .select(
                        col("parsed.order_id").as("order_id"),
                        col("parsed.user_id").as("user_id"),
                        col("parsed.order_amount").as("order_amount"));

        // 有效的：order_id 不为 null 且不为空
        Dataset<Row> valid = parsed.filter(
                col("order_id").isNotNull().and(col("order_id").notEqual("")));

        assertThat(valid.count()).isEqualTo(1);
        Row validRow = valid.first();
        assertThat(validRow.getString(0)).isEqualTo("ORD001");
    }

    @Test
    @DisplayName("fromJson_JSON缺失必填字段_Schema中标记为null而非解析失败")
    void fromJson_missingRequiredFields_producesNullValues() {
        String missingFieldsJson = "{\"order_id\":\"ORD100\"}";

        List<Row> rows = Arrays.asList(
                RowFactory.create("EVT100", "create", missingFieldsJson, "2026-06-09 10:00:00")
        );

        Dataset<Row> df = spark.createDataFrame(rows, new org.apache.spark.sql.types.StructType()
                .add("event_id", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_type", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_data", org.apache.spark.sql.types.DataTypes.StringType)
                .add("event_time", org.apache.spark.sql.types.DataTypes.StringType));

        Dataset<Row> parsed = df.withColumn("parsed",
                from_json(col("event_data"), OrderStatsConfig.ORDER_EVENT_SCHEMA));

        assertThat(parsed.filter(col("parsed").isNotNull()).count()).isEqualTo(1);
        Row result = parsed.selectExpr("parsed.order_id", "parsed.user_id", "parsed.order_amount")
                .first();
        assertThat(result.getString(0)).isEqualTo("ORD100");
        assertThat(result.isNullAt(1)).isTrue();
    }
}

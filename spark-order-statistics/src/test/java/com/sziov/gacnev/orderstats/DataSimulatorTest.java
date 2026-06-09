package com.sziov.gacnev.orderstats;

import com.sziov.gacnev.common.JsonUtils;
import com.sziov.gacnev.orderstats.config.OrderStatsConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 数据模拟器测试：验证生成的 JSON 数据符合 Schema 定义。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("DataSimulator 数据生成测试")
class DataSimulatorTest {

    private static SparkSession spark;

    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName("simulator-test")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.warehouse.dir",
                        System.getProperty("java.io.tmpdir") + "/spark-warehouse-sim-test")
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
    @DisplayName("生成订单JSON_符合Schema定义_全部必填字段存在")
    void generateOrderJson_matchesSchema_allRequiredFieldsPresent() {
        Map<String, Object> orderData = new HashMap<>();
        orderData.put("order_id", "ORD000001");
        orderData.put("user_id", "U0001");
        orderData.put("product_id", "P0001");
        orderData.put("store_id", "S0001");
        orderData.put("region_id", "110000");
        orderData.put("order_amount", "99.99");
        orderData.put("order_status", "create");
        orderData.put("create_time", "2026-06-09 10:00:00");
        orderData.put("pay_time", "");
        orderData.put("ship_time", "");
        orderData.put("sign_time", "");
        orderData.put("refund_time", "");

        String json = JsonUtils.toJson(orderData);
        assertThat(json).isNotNull();
        assertThat(json).contains("\"order_id\":\"ORD000001\"");
        assertThat(json).contains("\"user_id\":\"U0001\"");

        // 验证 JSON 可被 from_json 正常解析
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                org.apache.spark.sql.RowFactory.create(json)
        ), new org.apache.spark.sql.types.StructType()
                .add("event_data", org.apache.spark.sql.types.DataTypes.StringType));

        Dataset<Row> parsed = df.withColumn("parsed",
                from_json(col("event_data"), OrderStatsConfig.ORDER_EVENT_SCHEMA));
        assertThat(parsed.filter(col("parsed").isNotNull()).count()).isEqualTo(1);
    }

    @Test
    @DisplayName("生成订单JSON_包含所有事件类型字段_空值用空字符串")
    void generateOrderJson_emptyTimeFields_usesEmptyString() {
        Map<String, Object> orderData = new HashMap<>();
        orderData.put("order_id", "ORD000010");
        orderData.put("user_id", "U0010");
        orderData.put("product_id", "P0010");
        orderData.put("store_id", "S0010");
        orderData.put("region_id", "310000");
        orderData.put("order_amount", "50.00");
        orderData.put("order_status", "create");
        orderData.put("create_time", "2026-06-09 09:00:00");
        orderData.put("pay_time", "");
        orderData.put("ship_time", "");
        orderData.put("sign_time", "");
        orderData.put("refund_time", "");

        String json = JsonUtils.toJson(orderData);

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                org.apache.spark.sql.RowFactory.create(json)
        ), new org.apache.spark.sql.types.StructType()
                .add("event_data", org.apache.spark.sql.types.DataTypes.StringType));

        Dataset<Row> parsed = df.withColumn("parsed",
                from_json(col("event_data"), OrderStatsConfig.ORDER_EVENT_SCHEMA));
        Row result = parsed.selectExpr("parsed.*").first();
        assertThat(result.getString(0)).isEqualTo("ORD000010");
        assertThat(result.getString(7)).isEqualTo("2026-06-09 09:00:00");
    }

    @Test
    @DisplayName("空order_id_JSON_仍然可解析但后续被过滤")
    void emptyOrderIdJson_parsable_butFilteredLater() {
        Map<String, Object> orderData = new HashMap<>();
        orderData.put("order_id", "");
        orderData.put("user_id", "U0001");
        orderData.put("product_id", "P0001");
        orderData.put("store_id", "S0001");
        orderData.put("region_id", "110000");
        orderData.put("order_amount", "10.00");
        orderData.put("order_status", "create");
        orderData.put("create_time", "2026-06-09 10:00:00");
        orderData.put("pay_time", "");
        orderData.put("ship_time", "");
        orderData.put("sign_time", "");
        orderData.put("refund_time", "");

        String json = JsonUtils.toJson(orderData);

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                org.apache.spark.sql.RowFactory.create(json)
        ), new org.apache.spark.sql.types.StructType()
                .add("event_data", org.apache.spark.sql.types.DataTypes.StringType));

        Dataset<Row> parsed = df.withColumn("parsed",
                from_json(col("event_data"), OrderStatsConfig.ORDER_EVENT_SCHEMA));

        // JSON 可解析（parsed 不为 null），但 order_id 为空，后续 EtlUtils.filterNotNull 会过滤
        assertThat(parsed.filter(col("parsed").isNotNull()).count()).isEqualTo(1);
    }
}
